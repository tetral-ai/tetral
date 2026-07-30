package agentruntimebridge

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/agent"
	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/environment"
	"github.com/tetral-ai/tetral/internal/files"
	"github.com/tetral-ai/tetral/internal/memory"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/session"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	"github.com/tetral-ai/tetral/services/bridge/internal/outputcapture"
	tetralcleanup "github.com/tetral-ai/tetral/services/cleanup"
)

func TestSessionRuntimeLifecycleCreatePrepareRunIdleCleanupColdReturn(t *testing.T) {
	runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	client := dbconnect.NewClientForTesting(runtimeDB)
	ws := workspace.DefaultID
	bindingID := "bind_lifecycle_e2e"
	podUID := "pod_uid_lifecycle_e2e"

	seedLifecycleWorkspaceAgentEnvironment(t, adminDB, ws)
	seedLifecycleEnvironmentArtifact(t, adminDB, ws, "env_lifecycle", 1, "ready")

	sessionStore := session.NewPostgreSQLSessionStore(client, session.WithPageTokenSecret([]byte("lifecycle-page-token-secret")))
	sessionService := session.NewService(
		lifecycleAgentReader{},
		lifecycleEnvironmentReader{},
		files.NewService(files.NewPostgreSQLStore(client, nil)),
		lifecycleMemoryReader{},
		lifecycleVaultValidator{},
		sessionStore,
		lifecycleSessionEncryptor{},
		session.WithClock(func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }),
	)
	created, err := sessionService.Create(context.Background(), ws, session.CreateRequest{
		Agent:         session.AgentReference{ID: "agent_lifecycle"},
		EnvironmentID: "env_lifecycle",
	})
	if err != nil {
		t.Fatalf("Create session: %v", err)
	}
	sessionID := created.ID
	threadID := readLifecycleMainThreadID(t, adminDB, ws, sessionID)

	preparationAttemptID, sandboxID := readLifecyclePreparation(t, adminDB, ws, sessionID)
	assertLifecycleQueueJob(t, adminDB, ws, queue.KindSessionPrepare, queue.FormatSessionPrepareDedupeKey(ws, sessionID, preparationAttemptID), "pending")
	markLifecyclePreparationReady(t, adminDB, ws, sessionID, preparationAttemptID)
	seedLifecycleActiveSandbox(t, adminDB, ws, sessionID, sandboxID, "2026-01-01T00:00:00Z")
	seedLifecycleRuntimeBinding(t, adminDB, ws, sessionID, bindingID, 1, podUID)

	eventService := sessionevent.NewService(
		sessionevent.NewPostgreSQLStore(client),
		sessionevent.WithClock(func() time.Time { return time.Date(2026, 1, 1, 0, 0, 10, 0, time.UTC) }),
	)
	appended, err := eventService.AppendClientEvents(context.Background(), ws, sessionID, "idem_lifecycle_message", sessionevent.AppendRequest{
		Events: []sessionevent.IncomingEvent{{
			Type: sessionevent.EventTypeUserMessage,
			Content: []sessionevent.TextContentBlock{{
				Type: sessionevent.ContentBlockTypeText,
				Text: "please write the report",
			}},
		}},
	})
	if err != nil {
		t.Fatalf("AppendClientEvents: %v", err)
	}
	if len(appended.Data) != 1 {
		t.Fatalf("appended events = %d; want 1", len(appended.Data))
	}
	messageEventID := appended.Data[0].ID
	runtimeInputJob := readLifecycleRuntimeInputJob(t, adminDB, ws, sessionID)

	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(client, 9090)
	deliveryStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 20, 0, time.UTC) }
	sender := &lifecycleRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{
		Status:            agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED,
		SessionId:         sessionID,
		RuntimeInputId:    runtimeInputJob.RuntimeInputID,
		BindingId:         bindingID,
		BindingGeneration: 1,
	}}
	result, err := (RuntimePodDirectDeliverer{Store: deliveryStore, Sender: sender}).DeliverRuntimeJob(context.Background(), runtimeInputJob)
	if err != nil {
		t.Fatalf("DeliverRuntimeJob runtime input: %v", err)
	}
	if result.Status != RuntimeDeliveryAccepted || len(sender.requests) != 1 {
		t.Fatalf("runtime delivery result=%+v sender=%d; want accepted and one command", result, len(sender.requests))
	}

	bridgeStore := NewPostgreSQLBridgeAPIStore(client)
	if _, err := bridgeStore.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope:          lifecycleScope(sessionID, threadID, bindingID, 1, podUID),
		RuntimeInputId: runtimeInputJob.RuntimeInputID,
		EventIds:       []string{messageEventID},
		SequenceFrom:   sender.requests[0].GetSequenceFrom(),
		SequenceTo:     sender.requests[0].GetSequenceTo(),
		Drafts: []*bridgev1.RuntimeMessageDraft{
			bridgeUserInputDraftForTest(string(ws), sessionID, threadID, runtimeInputJob.RuntimeInputID, messageEventID, "build report"),
		},
	}); err != nil {
		t.Fatalf("CommitInputs: %v", err)
	}
	markLifecycleQueueJobAcknowledged(t, adminDB, ws, runtimeInputJob.JobID)
	assertLifecycleMessageProjected(t, adminDB, ws, sessionID, messageEventID)

	blobStore := blob.NewFakeBlobStore()
	bridgeStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC) }
	bridgeStore.SandboxStatusFreshnessWindow = time.Hour
	bridgeStore.OutputCapturer = outputcapture.NewCapturer(blobStore, &lifecycleOutputScanner{files: []outputcapture.SandboxOutputFile{
		lifecycleCapturedOutput("/mnt/session/outputs/report.txt", "report body"),
	}})
	idleScope := lifecycleScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIOpenDurableTurn(t, adminDB, idleScope, "evt_lifecycle_running")
	if _, err := bridgeStore.FinishIdle(context.Background(), &bridgev1.FinishIdleRequest{
		Scope:          idleScope,
		DurableTurnId:  "evt_lifecycle_running",
		StopReasonJson: `{"type":"end_turn"}`,
	}); err != nil {
		t.Fatalf("FinishIdle: %v", err)
	}
	assertLifecycleOutputCaptured(t, adminDB, blobStore, ws, sessionID)

	cleanupScheduler := tetralcleanup.NewScheduler(client)
	cleanupScheduler.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 1, 0, time.UTC) }
	cleanupScheduler.IDStrategy = lifecycleIDs("cleanup_lifecycle_1", "qjob_0000000000000001")
	claimed, err := cleanupScheduler.ClaimDue(context.Background(), tetralcleanup.ClaimDueRequest{WorkspaceID: ws, Limit: 10})
	if err != nil {
		t.Fatalf("cleanup ClaimDue: %v", err)
	}
	if len(claimed) != 1 || claimed[0].SessionID != sessionID || claimed[0].CleanupJobID != "cleanup_lifecycle_1" {
		t.Fatalf("claimed cleanup = %+v; want one lifecycle job", claimed)
	}

	cleanupJob := RuntimeJob{
		JobID:          "qjob_0000000000000001",
		LeaseToken:     "lease_lifecycle_cleanup",
		Kind:           queue.KindCleanupSession,
		WorkspaceID:    string(ws),
		SessionID:      sessionID,
		RuntimeInputID: "cleanup_session:cleanup_lifecycle_1",
		CleanupJobID:   "cleanup_lifecycle_1",
		CommandKind:    agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_CLEANUP_SESSION,
		PayloadJSON:    `{"workspace_id":"default","session_id":"` + sessionID + `","cleanup_job_id":"cleanup_lifecycle_1"}`,
	}
	deliveryStore.SandboxReleaser = &lifecycleSandboxReleaseClient{}
	deliveryStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 5, 0, time.UTC) }
	cleanupSender := &lifecycleRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{
		Status:            agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED,
		SessionId:         sessionID,
		RuntimeInputId:    cleanupJob.RuntimeInputID,
		BindingId:         bindingID,
		BindingGeneration: 1,
	}}
	cleanupResult, err := (RuntimePodDirectDeliverer{Store: deliveryStore, Sender: cleanupSender}).DeliverRuntimeJob(context.Background(), cleanupJob)
	if err != nil {
		t.Fatalf("DeliverRuntimeJob cleanup: %v", err)
	}
	if cleanupResult.Status != RuntimeDeliveryAccepted {
		t.Fatalf("cleanup delivery = %+v; want accepted", cleanupResult)
	}
	assertLifecycleCleanupFinalized(t, adminDB, ws, sessionID)
	assertLifecycleMessageProjected(t, adminDB, ws, sessionID, messageEventID)
	assertLifecycleOutputCaptured(t, adminDB, blobStore, ws, sessionID)

	coldEventService := sessionevent.NewService(
		sessionevent.NewPostgreSQLStore(client),
		sessionevent.WithClock(func() time.Time { return time.Date(2026, 1, 1, 0, 32, 0, 0, time.UTC) }),
	)
	coldAppended, err := coldEventService.AppendClientEvents(context.Background(), ws, sessionID, "idem_lifecycle_cold_return", sessionevent.AppendRequest{
		Events: []sessionevent.IncomingEvent{{
			Type: sessionevent.EventTypeUserMessage,
			Content: []sessionevent.TextContentBlock{{
				Type: sessionevent.ContentBlockTypeText,
				Text: "continue after cleanup",
			}},
		}},
	})
	if err != nil {
		t.Fatalf("AppendClientEvents cold return: %v", err)
	}
	if len(coldAppended.Data) != 1 {
		t.Fatalf("cold appended events = %d; want 1", len(coldAppended.Data))
	}
	coldEventID := coldAppended.Data[0].ID
	coldRuntimeInputJob := readLifecycleRuntimeInputJobForEvent(t, adminDB, ws, sessionID, coldEventID)
	deliveryStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 32, 5, 0, time.UTC) }
	coldSender := &lifecycleRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{
		Status:            agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED,
		SessionId:         sessionID,
		RuntimeInputId:    coldRuntimeInputJob.RuntimeInputID,
		BindingId:         bindingID,
		BindingGeneration: 1,
	}}
	coldResult, err := (RuntimePodDirectDeliverer{Store: deliveryStore, Sender: coldSender}).DeliverRuntimeJob(context.Background(), coldRuntimeInputJob)
	if err != nil {
		t.Fatalf("DeliverRuntimeJob cold return: %v", err)
	}
	if coldResult.Status != RuntimeDeliveryRejected || !coldResult.Retryable || coldResult.ErrorKind != "runtime_preparation_not_ready" || len(coldSender.requests) != 0 {
		t.Fatalf("cold delivery result=%+v sender=%d; want retryable preparation gate with no runtime command", coldResult, len(coldSender.requests))
	}
	assertLifecycleFreshPreparationQueued(t, adminDB, ws, sessionID, preparationAttemptID)
	assertLifecycleMessageProjected(t, adminDB, ws, sessionID, messageEventID)
	assertLifecycleOutputCaptured(t, adminDB, blobStore, ws, sessionID)
}

type lifecycleSessionEncryptor struct{}

func (lifecycleSessionEncryptor) Encrypt(plaintext []byte) ([]byte, error) {
	return append([]byte("encrypted:"), plaintext...), nil
}

func lifecycleScope(sessionID string, threadID string, bindingID string, generation int64, podUID string) *bridgev1.RuntimeScope {
	return &bridgev1.RuntimeScope{
		WorkspaceId:     string(workspace.DefaultID),
		SessionId:       sessionID,
		SessionThreadId: threadID,
		Binding: &bridgev1.RuntimeBindingRef{
			BindingId:         bindingID,
			BindingGeneration: generation,
			TargetPodUid:      podUID,
		},
	}
}

type lifecycleAgentReader struct{}

func (lifecycleAgentReader) Get(context.Context, workspace.ID, string) (*agent.Agent, error) {
	return lifecycleAgent(), nil
}

func (lifecycleAgentReader) GetVersion(context.Context, workspace.ID, string, int) (*agent.Agent, error) {
	return lifecycleAgent(), nil
}

func lifecycleAgent() *agent.Agent {
	return &agent.Agent{
		ID:      "agent_lifecycle",
		Type:    "agent",
		Version: 1,
		AgentConfig: agent.AgentConfig{
			Name:  "Lifecycle Agent",
			Model: "anthropic/claude-opus-4-8",
			Tools: agent.RawArray{
				json.RawMessage(`{"type":"tetral_agent_toolset","family":"claude"}`),
			},
		}.Normalize(),
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

type lifecycleEnvironmentReader struct{}

func (lifecycleEnvironmentReader) Get(context.Context, workspace.ID, string) (*environment.Environment, error) {
	return &environment.Environment{
		ID:                "env_lifecycle",
		Type:              "environment",
		Name:              "Lifecycle Environment",
		CurrentGeneration: 1,
		CreatedAt:         time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt:         time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}, nil
}

type lifecycleMemoryReader struct{}

func (lifecycleMemoryReader) GetStore(context.Context, workspace.ID, string) (*memory.Store, error) {
	return nil, nil
}

type lifecycleVaultValidator struct{}

func (lifecycleVaultValidator) ValidateVaultReferences(context.Context, workspace.ID, []string) error {
	return nil
}

type lifecycleRuntimeCommandSender struct {
	response *agentruntimev1.RuntimeInputCommandResponse
	requests []*agentruntimev1.RuntimeInputCommandRequest
}

func (s *lifecycleRuntimeCommandSender) SendRuntimeCommand(_ context.Context, _ RuntimePodTarget, request *agentruntimev1.RuntimeInputCommandRequest) (*agentruntimev1.RuntimeInputCommandResponse, error) {
	s.requests = append(s.requests, request)
	return s.response, nil
}

type lifecycleSandboxReleaseClient struct{}

func (lifecycleSandboxReleaseClient) ReleaseSandbox(context.Context, SandboxReleaseRequest) (SandboxReleaseResult, error) {
	return SandboxReleaseResult{
		Status:        SandboxReleaseReleased,
		SandboxStatus: "released",
	}, nil
}

type lifecycleOutputScanner struct {
	files []outputcapture.SandboxOutputFile
}

func (s *lifecycleOutputScanner) ScanOutputs(context.Context, outputcapture.SandboxOutputTarget) (outputcapture.SandboxOutputScan, error) {
	return outputcapture.SandboxOutputScan{Files: append([]outputcapture.SandboxOutputFile(nil), s.files...)}, nil
}

func lifecycleCapturedOutput(sourcePath string, body string) outputcapture.SandboxOutputFile {
	digest := sha256.Sum256([]byte(body))
	return outputcapture.SandboxOutputFile{
		SourcePath: sourcePath,
		Kind:       "regular",
		LinkCount:  1,
		SizeBytes:  int64(len(body)),
		SHA256:     hex.EncodeToString(digest[:]),
		MIMEType:   "text/plain",
		Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(body)), nil
		},
	}
}

func lifecycleIDs(values ...string) func(string) string {
	index := 0
	return func(prefix string) string {
		if index >= len(values) {
			return prefix + "extra"
		}
		value := values[index]
		index++
		return value
	}
}

func seedLifecycleWorkspaceAgentEnvironment(t *testing.T, db *sql.DB, ws workspace.ID) {
	t.Helper()
	now := "2026-01-01T00:00:00Z"
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO workspaces (id, type, name, created_at) VALUES ($1, 'workspace', 'default', $2) ON CONFLICT (id) DO NOTHING`, []any{string(ws), now}},
		{`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at) VALUES ($1, 'agent_lifecycle', 'Lifecycle Agent', 1, $2, $2)`, []any{string(ws), now}},
		{`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at) VALUES ($1, 'agv_lifecycle_1', 'agent_lifecycle', 1, '{"name":"Lifecycle Agent","model":"anthropic/claude-opus-4-8","tools":[{"type":"tetral_agent_toolset","family":"claude"}]}', 'hash-lifecycle-agent', $2)`, []any{string(ws), now}},
		{`INSERT INTO environments (workspace_id, id, name, config_json, current_generation, created_at, updated_at) VALUES ($1, 'env_lifecycle', 'Lifecycle Environment', '{}', 1, $2, $2)`, []any{string(ws), now}},
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(context.Background(), statement.query, statement.args...); err != nil {
			t.Fatalf("seed lifecycle reference: %v", err)
		}
	}
}

func seedLifecycleEnvironmentArtifact(t *testing.T, db *sql.DB, ws workspace.ID, environmentID string, generation int64, status string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO environment_artifacts (
			workspace_id, environment_id, generation, status, provider,
			provider_artifact_ref, normalized_config_hash, artifact_input_hash,
			runtime_network_policy_json, packages_json, created_at, updated_at
		) VALUES ($1, $2, $3, $4, 'tetral', $5, 'hash-config', 'hash-artifact', '{"type":"unrestricted"}', '{}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		string(ws), environmentID, generation, status, "artifact_"+environmentID); err != nil {
		t.Fatalf("seed lifecycle artifact: %v", err)
	}
}

func readLifecyclePreparation(t *testing.T, db *sql.DB, ws workspace.ID, sessionID string) (string, string) {
	t.Helper()
	var preparationAttemptID string
	var sandboxID string
	var status string
	if err := db.QueryRowContext(context.Background(),
		`SELECT preparation_attempt_id, sandbox_id, status
		   FROM session_preparations
		  WHERE workspace_id = $1 AND session_id = $2`,
		string(ws), sessionID).Scan(&preparationAttemptID, &sandboxID, &status); err != nil {
		t.Fatalf("read lifecycle preparation: %v", err)
	}
	if status != "pending" {
		t.Fatalf("preparation status = %q; want pending", status)
	}
	return preparationAttemptID, sandboxID
}

func readLifecycleMainThreadID(t *testing.T, db *sql.DB, ws workspace.ID, sessionID string) string {
	t.Helper()
	var threadID string
	if err := db.QueryRowContext(context.Background(),
		`SELECT main_thread_id
		   FROM sessions
		  WHERE workspace_id = $1 AND id = $2`,
		string(ws), sessionID).Scan(&threadID); err != nil {
		t.Fatalf("read lifecycle main thread: %v", err)
	}
	return threadID
}

func markLifecyclePreparationReady(t *testing.T, db *sql.DB, ws workspace.ID, sessionID string, preparationAttemptID string) {
	t.Helper()
	result, err := db.ExecContext(context.Background(),
		`UPDATE session_preparations
		    SET status = 'ready',
		        ready_at = '2026-01-01T00:00:00Z',
		        updated_at = '2026-01-01T00:00:00Z'
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND preparation_attempt_id = $3`,
		string(ws), sessionID, preparationAttemptID)
	if err != nil {
		t.Fatalf("mark preparation ready: %v", err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		t.Fatalf("mark preparation ready affected %d rows; want 1", affected)
	}
}

func seedLifecycleActiveSandbox(t *testing.T, db *sql.DB, ws workspace.ID, sessionID string, sandboxID string, refreshedAt string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO sandboxes (
			workspace_id, id, session_id, status, provider, provider_sandbox_id,
			machine_was_usable, created_at, updated_at, status_refreshed_at
		) VALUES ($1, $2, $3, 'active', 'tetral', $4, TRUE, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', $5)`,
		string(ws), sandboxID, sessionID, "provider_"+sessionID, refreshedAt); err != nil {
		t.Fatalf("seed lifecycle active sandbox: %v", err)
	}
}

func seedLifecycleRuntimeBinding(t *testing.T, db *sql.DB, ws workspace.ID, sessionID string, bindingID string, generation int64, podUID string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_runtime_bindings (
			workspace_id, session_id, binding_id, binding_generation, agent_runtime_namespace,
			agent_runtime_pod_name, agent_runtime_pod_uid, agent_runtime_pod_ip, bound_at, updated_at
		) VALUES ($1, $2, $3, $4, 'tetral-agent-runtime', 'runtime-pod-0', $5, '10.0.0.10', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		string(ws), sessionID, bindingID, generation, podUID); err != nil {
		t.Fatalf("seed lifecycle runtime binding: %v", err)
	}
}

func readLifecycleRuntimeInputJob(t *testing.T, db *sql.DB, ws workspace.ID, sessionID string) RuntimeJob {
	t.Helper()
	return readLifecycleRuntimeInputJobForEvent(t, db, ws, sessionID, "")
}

func readLifecycleRuntimeInputJobForEvent(t *testing.T, db *sql.DB, ws workspace.ID, sessionID string, eventID string) RuntimeJob {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT id, payload_json
		   FROM queue_jobs
		  WHERE workspace_id = $1
		    AND partition_key = $2
		    AND kind = $3
		    AND status = 'pending'
		  ORDER BY created_at ASC, id ASC`,
		string(ws), queue.FormatSessionPartitionKey(ws, sessionID), queue.KindRuntimeInput)
	if err != nil {
		t.Fatalf("read lifecycle runtime input queue job: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Fatalf("close lifecycle runtime input queue rows: %v", err)
		}
	}()
	for rows.Next() {
		var jobID string
		var payloadJSON string
		if err := rows.Scan(&jobID, &payloadJSON); err != nil {
			t.Fatalf("scan lifecycle runtime input queue job: %v", err)
		}
		var payload struct {
			WorkspaceID          string   `json:"workspace_id"`
			SessionID            string   `json:"session_id"`
			SessionThreadID      string   `json:"session_thread_id"`
			RuntimeInputID       string   `json:"runtime_input_id"`
			EventIDs             []string `json:"event_ids"`
			SequenceFrom         int64    `json:"sequence_from"`
			SequenceTo           int64    `json:"sequence_to"`
			InputKind            string   `json:"input_kind"`
			PreparationAttemptID string   `json:"preparation_attempt_id"`
		}
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			t.Fatalf("decode lifecycle runtime input payload: %v", err)
		}
		if eventID != "" {
			matched := false
			for _, candidate := range payload.EventIDs {
				if candidate == eventID {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		return RuntimeJob{
			JobID:                jobID,
			LeaseToken:           "lease_lifecycle_runtime_input",
			Kind:                 queue.KindRuntimeInput,
			WorkspaceID:          payload.WorkspaceID,
			SessionID:            payload.SessionID,
			SessionThreadID:      payload.SessionThreadID,
			RuntimeInputID:       payload.RuntimeInputID,
			PreparationAttemptID: payload.PreparationAttemptID,
			EventIDs:             payload.EventIDs,
			SequenceFrom:         payload.SequenceFrom,
			SequenceTo:           payload.SequenceTo,
			InputKind:            payload.InputKind,
			CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
			PayloadJSON:          payloadJSON,
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate lifecycle runtime input queue jobs: %v", err)
	}
	t.Fatalf("read lifecycle runtime input queue job for event %q: no pending job", eventID)
	return RuntimeJob{}
}

func markLifecycleQueueJobAcknowledged(t *testing.T, db *sql.DB, ws workspace.ID, jobID string) {
	t.Helper()
	result, err := db.ExecContext(context.Background(),
		`UPDATE queue_jobs
		    SET status = 'acknowledged',
		        acknowledged_at = '2026-01-01T00:00:30Z',
		        updated_at = '2026-01-01T00:00:30Z'
		  WHERE workspace_id = $1 AND id = $2`,
		string(ws), jobID)
	if err != nil {
		t.Fatalf("mark lifecycle queue job acknowledged: %v", err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		t.Fatalf("mark lifecycle queue job acknowledged affected %d rows; want 1", affected)
	}
}

func assertLifecycleQueueJob(t *testing.T, db *sql.DB, ws workspace.ID, kind string, dedupeKey string, status string) {
	t.Helper()
	var got string
	if err := db.QueryRowContext(context.Background(),
		`SELECT status FROM queue_jobs WHERE workspace_id = $1 AND kind = $2 AND dedupe_key = $3`,
		string(ws), kind, dedupeKey).Scan(&got); err != nil {
		t.Fatalf("read lifecycle queue job %s: %v", kind, err)
	}
	if got != status {
		t.Fatalf("queue job %s status = %q; want %q", kind, got, status)
	}
}

func assertLifecycleMessageProjected(t *testing.T, db *sql.DB, ws workspace.ID, sessionID string, eventID string) {
	t.Helper()
	var processedAt sql.NullString
	var messages int
	if err := db.QueryRowContext(context.Background(),
		`SELECT processed_at FROM session_events WHERE workspace_id = $1 AND session_id = $2 AND event_id = $3`,
		string(ws), sessionID, eventID).Scan(&processedAt); err != nil {
		t.Fatalf("read lifecycle event processed_at: %v", err)
	}
	if !processedAt.Valid {
		t.Fatalf("event %s processed_at is null; want committed input", eventID)
	}
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_messages WHERE workspace_id = $1 AND session_id = $2 AND source_event_id = $3 AND kind = 'user'`,
		string(ws), sessionID, eventID).Scan(&messages); err != nil {
		t.Fatalf("read lifecycle projected message: %v", err)
	}
	if messages != 1 {
		t.Fatalf("projected messages = %d; want 1", messages)
	}
}

func assertLifecycleOutputCaptured(t *testing.T, db *sql.DB, blobStore *blob.FakeBlobStore, ws workspace.ID, sessionID string) {
	t.Helper()
	var fileID string
	if err := db.QueryRowContext(context.Background(),
		`SELECT last_file_id
		   FROM session_output_captures
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND source_path = '/mnt/session/outputs/report.txt'`,
		string(ws), sessionID).Scan(&fileID); err != nil {
		t.Fatalf("read lifecycle output capture: %v", err)
	}
	var objectKey string
	if err := db.QueryRowContext(context.Background(),
		`SELECT o.blob_key
		   FROM files f
		   JOIN file_objects o ON o.workspace_id = f.workspace_id AND o.object_id = f.object_id
		  WHERE f.workspace_id = $1 AND f.file_id = $2`,
		string(ws), fileID).Scan(&objectKey); err != nil {
		t.Fatalf("read lifecycle output file: %v", err)
	}
	body, ok := blobStore.Bytes(objectKey)
	if !ok || string(body) != "report body" {
		t.Fatalf("output blob body = %q present=%v; want report body", string(body), ok)
	}
}

func assertLifecycleCleanupFinalized(t *testing.T, db *sql.DB, ws workspace.ID, sessionID string) {
	t.Helper()
	var bindingRows int
	var cleanupAfter sql.NullString
	var cleanupJobID sql.NullString
	var bindingID sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_runtime_bindings WHERE workspace_id = $1 AND session_id = $2`,
		string(ws), sessionID).Scan(&bindingRows); err != nil {
		t.Fatalf("read lifecycle binding rows: %v", err)
	}
	if bindingRows != 0 {
		t.Fatalf("binding rows after cleanup = %d; want 0", bindingRows)
	}
	if err := db.QueryRowContext(context.Background(),
		`SELECT cleanup_after, cleanup_job_id, binding_id
		   FROM session_runtime_status
		  WHERE workspace_id = $1 AND session_id = $2`,
		string(ws), sessionID).Scan(&cleanupAfter, &cleanupJobID, &bindingID); err != nil {
		t.Fatalf("read lifecycle runtime status cleanup fields: %v", err)
	}
	if cleanupAfter.Valid || cleanupJobID.Valid || bindingID.Valid {
		t.Fatalf("cleanup fields after finalization = %v/%v/%v; want nulls", cleanupAfter, cleanupJobID, bindingID)
	}
}

func assertLifecycleFreshPreparationQueued(t *testing.T, db *sql.DB, ws workspace.ID, sessionID string, oldAttemptID string) {
	t.Helper()
	var freshAttemptID string
	var freshStatus string
	var supersededAt sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT preparation_attempt_id, status
		   FROM session_preparations
		  WHERE workspace_id = $1 AND session_id = $2 AND superseded_at IS NULL
		  ORDER BY created_at DESC, preparation_attempt_id DESC
		  LIMIT 1`,
		string(ws), sessionID).Scan(&freshAttemptID, &freshStatus); err != nil {
		t.Fatalf("read fresh lifecycle preparation: %v", err)
	}
	if freshAttemptID == oldAttemptID || freshStatus != "pending" {
		t.Fatalf("fresh preparation = %s/%s; want new pending attempt", freshAttemptID, freshStatus)
	}
	if err := db.QueryRowContext(context.Background(),
		`SELECT superseded_at
		   FROM session_preparations
		  WHERE workspace_id = $1 AND session_id = $2 AND preparation_attempt_id = $3`,
		string(ws), sessionID, oldAttemptID).Scan(&supersededAt); err != nil {
		t.Fatalf("read old lifecycle preparation: %v", err)
	}
	if !supersededAt.Valid {
		t.Fatalf("old preparation %s superseded_at is null", oldAttemptID)
	}
	assertLifecycleQueueJob(t, db, ws, queue.KindSessionPrepare, queue.FormatSessionPrepareDedupeKey(ws, sessionID, freshAttemptID), "pending")
}
