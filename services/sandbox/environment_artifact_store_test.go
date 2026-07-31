package tetralsandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

func TestEnvironmentArtifactStoreBuildReadyEnqueuesFanout(t *testing.T) {
	runtime, admin := newEnvironmentArtifactStoreTestDB(t)
	ctx := context.Background()
	seedEnvironmentArtifactStoreEnvironment(t, admin, "ws_env_store", "env_build")
	seedEnvironmentArtifact(t, admin, "ws_env_store", "env_build", 7, "pending", "", `{"pip":["pandas==2.2.0"],"apt":["git"]}`)
	store := NewEnvironmentArtifactStore(dbconnect.NewClientForTesting(runtime))
	job := EnvironmentBuildJob{WorkspaceID: "ws_env_store", EnvironmentID: "env_build", Generation: 7}

	input, claimed, err := store.ClaimEnvironmentBuild(ctx, job, fixedEnvironmentStoreTime)
	if err != nil {
		t.Fatalf("ClaimEnvironmentBuild: %v", err)
	}
	if !claimed || input.ArtifactInputHash != "hash_packages" || input.NormalizedPackages["pip"][0] != "pandas==2.2.0" {
		t.Fatalf("input = %+v claimed=%v; want packages from durable artifact", input, claimed)
	}
	assertEnvironmentArtifactStatus(t, admin, "ws_env_store", "env_build", 7, "building", "")

	if err := store.MarkEnvironmentBuildReady(ctx, job, "snapshot_ready", fixedEnvironmentStoreTime.Add(time.Minute)); err != nil {
		t.Fatalf("MarkEnvironmentBuildReady: %v", err)
	}
	assertEnvironmentArtifactStatus(t, admin, "ws_env_store", "env_build", 7, "ready", "snapshot_ready")
	assertQueueJobCount(t, admin, "ws_env_store", "environment_ready_fanout", 1)
}

func TestEnvironmentArtifactReadyFanoutWakesWaitingSandboxActivation(t *testing.T) {
	runtime, admin := newEnvironmentArtifactStoreTestDB(t)
	seedSandboxExecutionStoreFixture(t, admin)
	ctx := context.Background()
	if _, err := admin.Exec(`UPDATE environment_artifacts
		SET status = 'building', provider_artifact_ref = NULL
		WHERE workspace_id = 'ws_execution_store' AND environment_id = 'env_execution_store' AND generation = 1`); err != nil {
		t.Fatalf("mark artifact building: %v", err)
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtime), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	var operationID string
	if err := admin.QueryRow(`SELECT waiting_activation_operation_id FROM session_runtime_tool_results
		WHERE workspace_id = 'ws_execution_store' AND tool_use_event_id = 'evt_execution_a'`).Scan(&operationID); err != nil {
		t.Fatalf("read waiting activation: %v", err)
	}
	store := NewEnvironmentArtifactStore(dbconnect.NewClientForTesting(runtime))
	job := EnvironmentBuildJob{WorkspaceID: "ws_execution_store", EnvironmentID: "env_execution_store", Generation: 1}
	if err := store.MarkEnvironmentBuildReady(ctx, job, "artifact_execution_store", fixedEnvironmentStoreTime); err != nil {
		t.Fatalf("MarkEnvironmentBuildReady: %v", err)
	}
	if _, err := store.FanoutReadyEnvironment(ctx, EnvironmentReadyFanoutJob(job), fixedEnvironmentStoreTime); err != nil {
		t.Fatalf("FanoutReadyEnvironment: %v", err)
	}
	var state string
	var queueJobID sql.NullString
	if err := admin.QueryRow(`SELECT state, queue_job_id FROM sandbox_lifecycle_operations
		WHERE workspace_id = $1 AND operation_id = $2`, job.WorkspaceID, operationID).Scan(&state, &queueJobID); err != nil {
		t.Fatalf("read activation after fanout: %v", err)
	}
	if state != "pending" || !queueJobID.Valid {
		t.Fatalf("activation after artifact ready = state %q queue %v; want pending with notification", state, queueJobID)
	}
}

func TestEnvironmentArtifactFailureSettlesWaitingSandboxActivation(t *testing.T) {
	runtime, admin := newEnvironmentArtifactStoreTestDB(t)
	seedSandboxExecutionStoreFixture(t, admin)
	ctx := context.Background()
	if _, err := admin.Exec(`UPDATE environment_artifacts
		SET status = 'building', provider_artifact_ref = NULL
		WHERE workspace_id = 'ws_execution_store' AND environment_id = 'env_execution_store' AND generation = 1`); err != nil {
		t.Fatalf("mark artifact building: %v", err)
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtime), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	store := NewEnvironmentArtifactStore(dbconnect.NewClientForTesting(runtime))
	job := EnvironmentBuildJob{WorkspaceID: "ws_execution_store", EnvironmentID: "env_execution_store", Generation: 1}
	if err := store.MarkEnvironmentBuildTerminalFailure(ctx, job, EnvironmentArtifactFailure{
		Stage: "build_artifact", LastErrorKind: "environment_artifact_failed", Reason: "artifact build failed",
	}, fixedEnvironmentStoreTime); err != nil {
		t.Fatalf("MarkEnvironmentBuildTerminalFailure: %v", err)
	}
	assertSandboxExecutionState(t, admin, "evt_execution_a", "terminal_unconsumed", 1)
}

func TestEnvironmentArtifactStoreBuildReadyAdvancesSameInputFollowers(t *testing.T) {
	runtime, admin := newEnvironmentArtifactStoreTestDB(t)
	ctx := context.Background()
	seedEnvironmentArtifactStoreEnvironment(t, admin, "ws_env_follow", "env_build")
	seedEnvironmentArtifact(t, admin, "ws_env_follow", "env_build", 7, "building", "", `{"pip":["pandas==2.2.0"]}`)
	seedEnvironmentArtifact(t, admin, "ws_env_follow", "env_build", 8, "pending", "", `{"pip":["pandas==2.2.0"]}`)
	store := NewEnvironmentArtifactStore(dbconnect.NewClientForTesting(runtime))

	if err := store.MarkEnvironmentBuildReady(ctx,
		EnvironmentBuildJob{WorkspaceID: "ws_env_follow", EnvironmentID: "env_build", Generation: 7},
		"snapshot_ready", fixedEnvironmentStoreTime,
	); err != nil {
		t.Fatalf("MarkEnvironmentBuildReady: %v", err)
	}
	assertEnvironmentArtifactStatus(t, admin, "ws_env_follow", "env_build", 7, "ready", "snapshot_ready")
	assertEnvironmentArtifactStatus(t, admin, "ws_env_follow", "env_build", 8, "ready", "snapshot_ready")
	assertQueueJobCount(t, admin, "ws_env_follow", "environment_ready_fanout", 2)
}

func TestEnvironmentArtifactStoreTerminalFailureFailsWaitingPreparations(t *testing.T) {
	runtime, admin := newEnvironmentArtifactStoreTestDB(t)
	ctx := context.Background()
	seedEnvironmentArtifactStoreEnvironment(t, admin, "ws_env_store", "env_build")
	seedEnvironmentArtifact(t, admin, "ws_env_store", "env_build", 7, "building", "", `{"pip":["pandas==2.2.0"]}`)
	seedEnvironmentArtifactStoreSession(t, admin, "ws_env_store", "sesn_waiting", "env_build")
	seedEnvironmentArtifactStorePreparation(t, admin, "ws_env_store", "sesn_waiting", "prep_waiting", "env_build", 7, "waiting_environment")
	seedEnvironmentArtifactStorePendingMessage(t, admin, "ws_env_store", "sesn_waiting", "evt_waiting")
	store := NewEnvironmentArtifactStore(dbconnect.NewClientForTesting(runtime))

	err := store.MarkEnvironmentBuildTerminalFailure(ctx,
		EnvironmentBuildJob{WorkspaceID: "ws_env_store", EnvironmentID: "env_build", Generation: 7},
		EnvironmentArtifactFailure{Stage: "build_artifact", LastErrorKind: "config_invalid", Reason: "bad packages", Retryable: false},
		fixedEnvironmentStoreTime,
	)
	if err != nil {
		t.Fatalf("MarkEnvironmentBuildTerminalFailure: %v", err)
	}
	assertEnvironmentArtifactStatus(t, admin, "ws_env_store", "env_build", 7, "failed", "")
	status, failureReason := readPreparationStatusAndReason(t, admin, "ws_env_store", "sesn_waiting", "prep_waiting")
	if status != "failed" || failureReason != "bad packages" {
		t.Fatalf("preparation status/reason = %q/%q; want failed/bad packages", status, failureReason)
	}
	assertQueueJobCount(t, admin, "ws_env_store", "environment_failed_fanout", 1)

	fannedOut, err := store.FanoutFailedEnvironment(ctx,
		EnvironmentFailedFanoutJob{WorkspaceID: "ws_env_store", EnvironmentID: "env_build", Generation: 7},
		fixedEnvironmentStoreTime.Add(time.Minute),
	)
	if err != nil {
		t.Fatalf("FanoutFailedEnvironment: %v", err)
	}
	if fannedOut != 1 {
		t.Fatalf("fanned out preparations = %d; want 1", fannedOut)
	}
	var runtimeInputPayload string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json
		   FROM queue_jobs
		  WHERE workspace_id = 'ws_env_store'
		    AND kind = 'runtime_input'`,
	).Scan(&runtimeInputPayload); err != nil {
		t.Fatalf("read failed-environment runtime input: %v", err)
	}
	var payload struct {
		EventIDs             []string `json:"event_ids"`
		PreparationAttemptID string   `json:"preparation_attempt_id"`
	}
	if err := json.Unmarshal([]byte(runtimeInputPayload), &payload); err != nil {
		t.Fatalf("decode failed-environment runtime input: %v", err)
	}
	if !reflect.DeepEqual(payload.EventIDs, []string{"evt_waiting"}) || payload.PreparationAttemptID != "prep_waiting" {
		t.Fatalf("failed-environment runtime input = %+v; want event fence for prep_waiting", payload)
	}
}

func TestEnvironmentArtifactStoreTerminalFailureFailsSameInputFollowers(t *testing.T) {
	runtime, admin := newEnvironmentArtifactStoreTestDB(t)
	ctx := context.Background()
	seedEnvironmentArtifactStoreEnvironment(t, admin, "ws_env_fail_follow", "env_build")
	seedEnvironmentArtifact(t, admin, "ws_env_fail_follow", "env_build", 7, "building", "", `{"pip":["pandas==2.2.0"]}`)
	seedEnvironmentArtifact(t, admin, "ws_env_fail_follow", "env_build", 8, "pending", "", `{"pip":["pandas==2.2.0"]}`)
	seedEnvironmentArtifactStoreSession(t, admin, "ws_env_fail_follow", "sesn_waiting", "env_build")
	seedEnvironmentArtifactStorePreparation(t, admin, "ws_env_fail_follow", "sesn_waiting", "prep_waiting", "env_build", 8, "waiting_environment")
	store := NewEnvironmentArtifactStore(dbconnect.NewClientForTesting(runtime))

	if err := store.MarkEnvironmentBuildTerminalFailure(ctx,
		EnvironmentBuildJob{WorkspaceID: "ws_env_fail_follow", EnvironmentID: "env_build", Generation: 7},
		EnvironmentArtifactFailure{Stage: "build_artifact", LastErrorKind: "config_invalid", Reason: "bad packages"},
		fixedEnvironmentStoreTime,
	); err != nil {
		t.Fatalf("MarkEnvironmentBuildTerminalFailure: %v", err)
	}
	assertEnvironmentArtifactStatus(t, admin, "ws_env_fail_follow", "env_build", 7, "failed", "")
	assertEnvironmentArtifactStatus(t, admin, "ws_env_fail_follow", "env_build", 8, "failed", "")
	status, reason := readPreparationStatusAndReason(t, admin, "ws_env_fail_follow", "sesn_waiting", "prep_waiting")
	if status != "failed" || reason != "bad packages" {
		t.Fatalf("follower preparation = %q/%q; want failed/bad packages", status, reason)
	}
	assertQueueJobCount(t, admin, "ws_env_fail_follow", "environment_failed_fanout", 1)
}

func TestEnvironmentArtifactStoreFailedFanoutCopiesRecordedBirthAfterSuccessorAllocation(t *testing.T) {
	runtime, admin := newEnvironmentArtifactStoreTestDB(t)
	ctx := context.Background()
	const (
		workspaceID = "ws_env_stale_fanout"
		sessionID   = "sesn_stale_fanout"
	)
	seedEnvironmentArtifactStoreEnvironment(t, admin, workspaceID, "env_build")
	seedEnvironmentArtifact(t, admin, workspaceID, "env_build", 7, "failed", "", `{"pip":["pandas==2.2.0"]}`)
	seedEnvironmentArtifactStoreSession(t, admin, workspaceID, sessionID, "env_build")
	seedEnvironmentArtifactStorePreparation(t, admin, workspaceID, sessionID, "prep_failed_old", "env_build", 7, "failed")
	seedEnvironmentArtifactStorePendingMessage(t, admin, workspaceID, sessionID, "evt_stale_fanout")
	if _, err := admin.ExecContext(ctx,
		`UPDATE session_events
		    SET preparation_attempt_id = 'prep_failed_old'
		  WHERE workspace_id = $1
		    AND event_id = 'evt_stale_fanout'`,
		workspaceID,
	); err != nil {
		t.Fatalf("stamp failed-attempt event birth: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		`INSERT INTO session_preparations (
			workspace_id, session_id, preparation_attempt_id, environment_id, environment_generation,
			sandbox_id, status, created_at, updated_at
		) VALUES (
			$1, $2, 'prep_successor', 'env_build', 8,
			'sandbox_' || $2, 'pending', '2026-05-22T10:01:00Z', '2026-05-22T10:01:00Z'
		)`,
		workspaceID, sessionID,
	); err != nil {
		t.Fatalf("seed successor preparation: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		`UPDATE session_preparations
		    SET superseded_at = '2026-05-22T10:01:00Z',
		        updated_at = '2026-05-22T10:01:00Z'
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND preparation_attempt_id = 'prep_failed_old'`,
		workspaceID, sessionID,
	); err != nil {
		t.Fatalf("supersede failed preparation: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			preparation_attempt_id, created_at, updated_at
		) VALUES (
			$1, $2, 'thr_' || $2, 'evt_successor_fanout', 2, 'user.message',
			'{"type":"user.message","content":[{"type":"text","text":"after failure"}]}',
			'prep_successor', '2026-05-22T10:01:00Z', '2026-05-22T10:01:00Z'
		)`,
		workspaceID, sessionID,
	); err != nil {
		t.Fatalf("seed successor event: %v", err)
	}
	store := NewEnvironmentArtifactStore(dbconnect.NewClientForTesting(runtime))
	fannedOut, err := store.FanoutFailedEnvironment(ctx,
		EnvironmentFailedFanoutJob{WorkspaceID: workspaceID, EnvironmentID: "env_build", Generation: 7},
		fixedEnvironmentStoreTime.Add(time.Minute),
	)
	if err != nil {
		t.Fatalf("FanoutFailedEnvironment: %v", err)
	}
	if fannedOut != 1 {
		t.Fatalf("fanned out historical attempts = %d; want 1 from recorded birth", fannedOut)
	}
	var runtimeInputCount int
	if err := admin.QueryRowContext(ctx,
		`SELECT COUNT(*)
		   FROM queue_jobs
		  WHERE workspace_id = $1
		    AND kind = 'runtime_input'`,
		workspaceID,
	).Scan(&runtimeInputCount); err != nil {
		t.Fatalf("count failed-attempt runtime inputs: %v", err)
	}
	if runtimeInputCount != 1 {
		t.Fatalf("failed-attempt runtime inputs = %d; want exactly 1 with successor birth excluded", runtimeInputCount)
	}
	var payloadJSON string
	if err := admin.QueryRowContext(ctx,
		`SELECT payload_json
		   FROM queue_jobs
		  WHERE workspace_id = $1
		    AND kind = 'runtime_input'`,
		workspaceID,
	).Scan(&payloadJSON); err != nil {
		t.Fatalf("read failed-attempt runtime input: %v", err)
	}
	var payload struct {
		EventIDs             []string `json:"event_ids"`
		PreparationAttemptID string   `json:"preparation_attempt_id"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode failed-attempt runtime input: %v", err)
	}
	if !reflect.DeepEqual(payload.EventIDs, []string{"evt_stale_fanout"}) || payload.PreparationAttemptID != "prep_failed_old" {
		t.Fatalf("failed-attempt runtime input = %+v; want recorded prep_failed_old birth", payload)
	}
}

func TestEnvironmentArtifactStoreFanoutMovesWaitingRowsAndEnqueuesPrepare(t *testing.T) {
	runtime, admin := newEnvironmentArtifactStoreTestDB(t)
	ctx := context.Background()
	seedEnvironmentArtifactStoreEnvironment(t, admin, "ws_env_store", "env_build")
	seedEnvironmentArtifact(t, admin, "ws_env_store", "env_build", 7, "ready", "snapshot_ready", `{"pip":["pandas==2.2.0"]}`)
	seedEnvironmentArtifactStoreSession(t, admin, "ws_env_store", "sesn_waiting", "env_build")
	seedEnvironmentArtifactStoreSession(t, admin, "ws_env_store", "sesn_waiting_second", "env_build")
	seedEnvironmentArtifactStorePreparation(t, admin, "ws_env_store", "sesn_waiting", "prep_waiting", "env_build", 7, "waiting_environment")
	seedEnvironmentArtifactStorePreparation(t, admin, "ws_env_store", "sesn_waiting_second", "prep_waiting_second", "env_build", 7, "waiting_environment")
	store := NewEnvironmentArtifactStore(dbconnect.NewClientForTesting(runtime))

	advanced, err := store.FanoutReadyEnvironment(ctx, EnvironmentReadyFanoutJob{WorkspaceID: "ws_env_store", EnvironmentID: "env_build", Generation: 7}, fixedEnvironmentStoreTime)
	if err != nil {
		t.Fatalf("FanoutReadyEnvironment: %v", err)
	}
	if advanced != 2 {
		t.Fatalf("advanced = %d; want 2", advanced)
	}
	status, _ := readPreparationStatusAndReason(t, admin, "ws_env_store", "sesn_waiting", "prep_waiting")
	if status != "pending" {
		t.Fatalf("preparation status = %q; want pending", status)
	}
	assertQueueJobCount(t, admin, "ws_env_store", "session_prepare", 2)
	var firstPartitionJobs int
	if err := admin.QueryRowContext(ctx,
		`SELECT COUNT(*)
		   FROM queue_jobs
		  WHERE workspace_id = $1
		    AND kind = 'session_prepare'
		    AND queue_partition_sequence = 1`,
		"ws_env_store",
	).Scan(&firstPartitionJobs); err != nil {
		t.Fatalf("read preparation partition sequences: %v", err)
	}
	if firstPartitionJobs != 2 {
		t.Fatalf("preparation jobs with first partition sequence = %d; want 2", firstPartitionJobs)
	}
}

func newEnvironmentArtifactStoreTestDB(t *testing.T) (runtime *sql.DB, admin *sql.DB) {
	t.Helper()
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	return storagetest.NewPostgreSQLDBWithAdmin(t)
}

func seedEnvironmentArtifactStoreEnvironment(t *testing.T, db *sql.DB, workspaceID string, environmentID string) {
	t.Helper()
	now := fixedEnvironmentStoreTime.Format(time.RFC3339)
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO agents (workspace_id, id, name, description, version, created_at, updated_at)
		 VALUES ($1, 'agent_env_store', 'Environment Store Agent', '', 1, $2, $2)
		 ON CONFLICT (workspace_id, id) DO NOTHING`,
		workspaceID, now,
	); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at)
		 VALUES ($1, 'agentver_env_store', 'agent_env_store', 1, '{}', 'hash', $2)
		 ON CONFLICT (workspace_id, agent_id, version) DO NOTHING`,
		workspaceID, now,
	); err != nil {
		t.Fatalf("seed agent version: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO environments (workspace_id, id, name, description, config_json, current_generation, metadata_json, created_at, updated_at)
		 VALUES ($1, $2, $2, '', '{"type":"cloud","networking":{"type":"unrestricted"},"packages":{}}', 7, '{}', $3, $3)
		 ON CONFLICT (workspace_id, id) DO NOTHING`,
		workspaceID, environmentID, now,
	); err != nil {
		t.Fatalf("seed environment: %v", err)
	}
}

func seedEnvironmentArtifact(t *testing.T, db *sql.DB, workspaceID string, environmentID string, generation int64, status string, providerRef string, packagesJSON string) {
	t.Helper()
	now := fixedEnvironmentStoreTime.Format(time.RFC3339)
	_, err := db.ExecContext(context.Background(),
		`INSERT INTO environment_artifacts (
			workspace_id, environment_id, generation, status, provider,
			provider_artifact_ref, normalized_config_hash, artifact_input_hash,
			runtime_network_policy_json, packages_json, created_at, updated_at
		) VALUES ($1, $2, $3, $4, 'daytona', NULLIF($5, ''), 'hash_config', 'hash_packages',
			'{"type":"unrestricted"}', $6, $7::timestamptz, $7::timestamptz)`,
		workspaceID, environmentID, generation, status, providerRef, packagesJSON, now,
	)
	if err != nil {
		t.Fatalf("seed environment artifact: %v", err)
	}
}

func seedEnvironmentArtifactStoreSession(t *testing.T, db *sql.DB, workspaceID string, sessionID string, environmentID string) {
	t.Helper()
	now := fixedEnvironmentStoreTime.Format(time.RFC3339)
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO sessions (
			workspace_id, id, type, title, metadata_json, status, agent_id, agent_version,
			environment_id, vault_ids_json, created_at, updated_at
		) VALUES ($1, $2, 'session', NULL, '{}', 'idle', 'agent_env_store', 1, $3, '[]', $4, $4)`,
		workspaceID, sessionID, environmentID, now,
	); err != nil {
		t.Fatalf("seed session: %v", err)
	}
}

func seedEnvironmentArtifactStorePreparation(t *testing.T, db *sql.DB, workspaceID string, sessionID string, preparationID string, environmentID string, generation int64, status string) {
	t.Helper()
	now := fixedEnvironmentStoreTime.Format(time.RFC3339)
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_preparations (
			workspace_id, session_id, preparation_attempt_id, environment_id, environment_generation,
			sandbox_id, status, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'sandbox_env_store', $6, $7, $7)`,
		workspaceID, sessionID, preparationID, environmentID, generation, status, now,
	); err != nil {
		t.Fatalf("seed session preparation: %v", err)
	}
}

func seedEnvironmentArtifactStorePendingMessage(t *testing.T, db *sql.DB, workspaceID string, sessionID string, eventID string) {
	t.Helper()
	threadID := "thr_" + sessionID
	now := fixedEnvironmentStoreTime.Format(time.RFC3339)
	var preparationAttemptID string
	if err := db.QueryRowContext(context.Background(),
		`SELECT preparation_attempt_id
		   FROM session_preparations
		  WHERE workspace_id = $1
		    AND session_id = $2
		  ORDER BY created_at DESC, preparation_attempt_id DESC
		  LIMIT 1`,
		workspaceID,
		sessionID,
	).Scan(&preparationAttemptID); err != nil {
		t.Fatalf("read environment artifact event birth: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`UPDATE sessions
		    SET main_thread_id = $3,
		        lifecycle_state = 'active'
		  WHERE workspace_id = $1
		    AND id = $2`,
		workspaceID, sessionID, threadID,
	); err != nil {
		t.Fatalf("seed environment artifact session main thread: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_threads (
			workspace_id, id, session_id, role, visibility, status, created_at, last_active_at, updated_at
		) VALUES ($1, $2, $3, 'main', 'public', 'idle', $4, $4, $4)`,
		workspaceID, threadID, sessionID, now,
	); err != nil {
		t.Fatalf("seed environment artifact session thread: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			preparation_attempt_id, created_at, updated_at
		) VALUES ($1, $2, $3, $4, 1, 'user.message',
		          '{"type":"user.message","content":[{"type":"text","text":"continue"}]}',
		          $5, $6, $6)`,
		workspaceID, sessionID, threadID, eventID, preparationAttemptID, now,
	); err != nil {
		t.Fatalf("seed environment artifact pending message: %v", err)
	}
	var streamPosition int64
	if err := db.QueryRowContext(context.Background(),
		`INSERT INTO session_event_stream_changes (
			workspace_id, session_id, event_id, session_thread_id, revision,
			visibility, session_visible, changed_at
		) VALUES ($1, $2, $3, $4, 1, 'public', TRUE, $5)
		RETURNING stream_position`,
		workspaceID, sessionID, eventID, threadID, now,
	).Scan(&streamPosition); err != nil {
		t.Fatalf("seed environment artifact stream change: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`UPDATE session_events
		    SET insert_stream_position = $5
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND event_id = $4`,
		workspaceID, sessionID, threadID, eventID, streamPosition,
	); err != nil {
		t.Fatalf("seed environment artifact insert stream position: %v", err)
	}
}

func assertEnvironmentArtifactStatus(t *testing.T, db *sql.DB, workspaceID string, environmentID string, generation int64, wantStatus string, wantProviderRef string) {
	t.Helper()
	var status string
	var providerRef sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT status, provider_artifact_ref
		   FROM environment_artifacts
		  WHERE workspace_id = $1 AND environment_id = $2 AND generation = $3`,
		workspaceID, environmentID, generation,
	).Scan(&status, &providerRef); err != nil {
		t.Fatalf("read environment artifact: %v", err)
	}
	if status != wantStatus || providerRef.String != wantProviderRef {
		t.Fatalf("artifact status/ref = %q/%q; want %q/%q", status, providerRef.String, wantStatus, wantProviderRef)
	}
}

func assertQueueJobCount(t *testing.T, db *sql.DB, workspaceID string, kind string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM queue_jobs WHERE workspace_id = $1 AND kind = $2`,
		workspaceID, kind,
	).Scan(&got); err != nil {
		t.Fatalf("count queue jobs: %v", err)
	}
	if got != want {
		t.Fatalf("queue job count kind %s = %d; want %d", kind, got, want)
	}
}

func readPreparationStatusAndReason(t *testing.T, db *sql.DB, workspaceID string, sessionID string, preparationID string) (string, string) {
	t.Helper()
	var status string
	var reason sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT status, failure_reason
		   FROM session_preparations
		  WHERE workspace_id = $1 AND session_id = $2 AND preparation_attempt_id = $3`,
		workspaceID, sessionID, preparationID,
	).Scan(&status, &reason); err != nil {
		t.Fatalf("read session preparation: %v", err)
	}
	return status, reason.String
}

var fixedEnvironmentStoreTime = time.Date(2026, 5, 22, 10, 0, 0, 0, time.UTC)
