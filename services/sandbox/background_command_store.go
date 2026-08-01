package tetralsandbox

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

type PostgreSQLSandboxBackgroundCommandStore struct {
	client *dbconnect.Client
}

func NewPostgreSQLSandboxBackgroundCommandStore(client *dbconnect.Client) *PostgreSQLSandboxBackgroundCommandStore {
	return &PostgreSQLSandboxBackgroundCommandStore{client: client}
}

func (s *PostgreSQLSandboxBackgroundCommandStore) LoadReconcile(ctx context.Context, job SandboxBackgroundReconcileJob) (SandboxBackgroundTaskWork, bool, error) {
	var work SandboxBackgroundTaskWork
	var current bool
	err := s.withWorkspaceTx(ctx, job.WorkspaceID, "sandbox.background.load_reconcile", func(tx *dbconnect.Tx) error {
		if err := lockBackgroundSessionTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
			return err
		}
		loaded, found, err := loadBackgroundTaskForUpdateTx(ctx, tx, job.WorkspaceID, job.SessionID, job.TaskID)
		if err != nil || !found {
			return err
		}
		work = loaded
		current = loaded.ReconcileGeneration == job.ReconcileGeneration
		return nil
	})
	return work, current, err
}

func (s *PostgreSQLSandboxBackgroundCommandStore) AdvanceReconcile(ctx context.Context, work SandboxBackgroundTaskWork, _ sandboxdriver.CommandResult, now time.Time, nextPoll time.Time) error {
	if now.IsZero() || nextPoll.IsZero() || nextPoll.Before(now) {
		return errors.New("background reconcile times are invalid")
	}
	return s.withWorkspaceTx(ctx, work.WorkspaceID, "sandbox.background.advance_reconcile", func(tx *dbconnect.Tx) error {
		if err := lockBackgroundSessionTx(ctx, tx, work.WorkspaceID, work.SessionID); err != nil {
			return err
		}
		var status string
		var generation int64
		if err := tx.QueryRow(ctx, `SELECT status, reconcile_generation FROM session_background_tasks
			WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3 FOR UPDATE`, work.WorkspaceID, work.SessionID, work.TaskID).Scan(&status, &generation); dbconnect.IsNoRows(err) {
			return nil
		} else if err != nil {
			return err
		}
		if status != "running" || generation != work.ReconcileGeneration {
			return nil
		}
		nextGeneration := generation + 1
		if _, err := tx.Exec(ctx, `UPDATE session_background_tasks SET reconcile_generation=$4, next_poll_at=$5, updated_at=$6
			WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3 AND status='running' AND reconcile_generation=$7`,
			work.WorkspaceID, work.SessionID, work.TaskID, nextGeneration, nextPoll.UTC(), now.UTC(), generation); err != nil {
			return err
		}
		return enqueueBackgroundReconcileTx(ctx, tx, work.WorkspaceID, work.SessionID, work.TaskID, nextGeneration, now.UTC(), nextPoll.UTC())
	})
}

func (s *PostgreSQLSandboxBackgroundCommandStore) SettleTask(ctx context.Context, work SandboxBackgroundTaskWork, result sandboxdriver.CommandResult, now time.Time) error {
	if result.TerminalStatus == "" || result.ResultJSON == "" || !json.Valid([]byte(result.ResultJSON)) {
		return errors.New("background task terminal result is invalid")
	}
	result.ResultJSON = normalizeSandboxProviderResult(result.ResultJSON)
	return s.withWorkspaceTx(ctx, work.WorkspaceID, "sandbox.background.settle_task", func(tx *dbconnect.Tx) error {
		if err := lockBackgroundSessionTx(ctx, tx, work.WorkspaceID, work.SessionID); err != nil {
			return err
		}
		return settleBackgroundTaskResultTx(ctx, tx, work, result, now.UTC())
	})
}

func (s *PostgreSQLSandboxBackgroundCommandStore) FinalizeReconcileExhaustion(ctx context.Context, job *queuev1.QueueJob, now time.Time) error {
	if job == nil || job.GetKind() != queue.KindSandboxBackgroundReconcile {
		return errors.New("sandbox background reconcile Queue transport identity is invalid")
	}
	identity, err := decodeBackgroundQueueIdentity(job.GetWorkspaceId(), job.GetKind(), job.GetPartitionKey(), job.GetDedupeKey())
	if err != nil {
		return err
	}
	return s.withWorkspaceTx(ctx, identity.WorkspaceID, "sandbox.background.finalize_reconcile_exhaustion", func(tx *dbconnect.Tx) error {
		return finalizeExhaustedBackgroundReconcileTx(ctx, tx, identity, now.UTC())
	})
}

func (s *PostgreSQLSandboxBackgroundCommandStore) LoadCommand(ctx context.Context, job SandboxBackgroundCommandJob, now time.Time) (SandboxBackgroundOperationWork, bool, error) {
	var work SandboxBackgroundOperationWork
	var current bool
	err := s.withWorkspaceTx(ctx, job.WorkspaceID, "sandbox.background.load_command", func(tx *dbconnect.Tx) error {
		if err := lockBackgroundSessionTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
			return err
		}
		var err error
		work, current, err = loadBackgroundCommandForUpdateTx(ctx, tx, job, now.UTC())
		return err
	})
	return work, current, err
}

func (s *PostgreSQLSandboxBackgroundCommandStore) MarkCommandSubmitted(ctx context.Context, work SandboxBackgroundOperationWork, now time.Time) (bool, error) {
	var changed bool
	err := s.withWorkspaceTx(ctx, work.Task.WorkspaceID, "sandbox.background.mark_submitted", func(tx *dbconnect.Tx) error {
		if err := lockBackgroundSessionTx(ctx, tx, work.Task.WorkspaceID, work.Task.SessionID); err != nil {
			return err
		}
		result, err := tx.Exec(ctx, `UPDATE session_runtime_tool_results
			SET background_operation_state='submitted', updated_at=$6
			WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
			  AND tool_use_event_id=$4 AND background_request_id=$5 AND background_operation_state='pending'`,
			work.Task.WorkspaceID, work.Task.SessionID, work.Task.SessionThreadID,
			work.ToolUseEventID, work.RequestID, now.UTC())
		if err != nil {
			return err
		}
		changed = transitionRowsAffected(result)
		return nil
	})
	return changed, err
}

func (s *PostgreSQLSandboxBackgroundCommandStore) SettleCommand(ctx context.Context, work SandboxBackgroundOperationWork, disposition string, result sandboxdriver.CommandResult, now time.Time) error {
	if result.ResultJSON == "" || !json.Valid([]byte(result.ResultJSON)) {
		return errors.New("background command result is invalid")
	}
	result.ResultJSON = normalizeSandboxProviderResult(result.ResultJSON)
	return s.withWorkspaceTx(ctx, work.Task.WorkspaceID, "sandbox.background.settle_command", func(tx *dbconnect.Tx) error {
		if err := lockBackgroundSessionTx(ctx, tx, work.Task.WorkspaceID, work.Task.SessionID); err != nil {
			return err
		}
		return settleBackgroundCommandTx(ctx, tx, work, disposition, result, now.UTC())
	})
}

func (s *PostgreSQLSandboxBackgroundCommandStore) FinalizeCommandExhaustion(ctx context.Context, job *queuev1.QueueJob, now time.Time) error {
	if job == nil || job.GetKind() != queue.KindSandboxBackgroundCommand {
		return errors.New("sandbox background command Queue transport identity is invalid")
	}
	identity, err := decodeBackgroundQueueIdentity(job.GetWorkspaceId(), job.GetKind(), job.GetPartitionKey(), job.GetDedupeKey())
	if err != nil {
		return err
	}
	return s.withWorkspaceTx(ctx, identity.WorkspaceID, "sandbox.background.finalize_command_exhaustion", func(tx *dbconnect.Tx) error {
		return finalizeExhaustedBackgroundCommandTx(ctx, tx, identity, now.UTC())
	})
}

type backgroundQueueIdentity struct {
	WorkspaceID string
	SessionID   string
	TaskID      string
	RequestID   string
	Generation  int64
}

func decodeBackgroundQueueIdentity(workspaceID string, kind string, partitionKey string, dedupeKey string) (backgroundQueueIdentity, error) {
	prefix := "sandbox-background:" + workspaceID + ":"
	if workspaceID == "" || !strings.HasPrefix(partitionKey, prefix) {
		return backgroundQueueIdentity{}, errors.New("sandbox background Queue partition identity is invalid")
	}
	parts := strings.Split(strings.TrimPrefix(partitionKey, prefix), ":")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return backgroundQueueIdentity{}, errors.New("sandbox background Queue partition identity is invalid")
	}
	identity := backgroundQueueIdentity{WorkspaceID: workspaceID, SessionID: parts[0], TaskID: parts[1]}
	switch kind {
	case queue.KindSandboxBackgroundReconcile:
		prefix := queue.KindSandboxBackgroundReconcile + ":" + workspaceID + ":" + identity.SessionID + ":" + identity.TaskID + ":"
		if !strings.HasPrefix(dedupeKey, prefix) {
			return backgroundQueueIdentity{}, errors.New("sandbox background reconcile Queue identity is invalid")
		}
		generation, err := strconv.ParseInt(strings.TrimPrefix(dedupeKey, prefix), 10, 64)
		if err != nil || generation <= 0 || dedupeKey != queue.FormatSandboxBackgroundReconcileDedupeKey(workspace.ID(workspaceID), identity.SessionID, identity.TaskID, generation) {
			return backgroundQueueIdentity{}, errors.New("sandbox background reconcile Queue identity is invalid")
		}
		identity.Generation = generation
	case queue.KindSandboxBackgroundCommand:
		prefix := queue.KindSandboxBackgroundCommand + ":" + workspaceID + ":" + identity.SessionID + ":" + identity.TaskID + ":"
		if !strings.HasPrefix(dedupeKey, prefix) {
			return backgroundQueueIdentity{}, errors.New("sandbox background command Queue identity is invalid")
		}
		identity.RequestID = strings.TrimPrefix(dedupeKey, prefix)
		if identity.RequestID == "" || dedupeKey != queue.FormatSandboxBackgroundCommandDedupeKey(workspace.ID(workspaceID), identity.SessionID, identity.TaskID, identity.RequestID) {
			return backgroundQueueIdentity{}, errors.New("sandbox background command Queue identity is invalid")
		}
	default:
		return backgroundQueueIdentity{}, errors.New("sandbox background Queue kind is invalid")
	}
	return identity, nil
}

func finalizeExhaustedBackgroundReconcileTx(ctx context.Context, tx *dbconnect.Tx, identity backgroundQueueIdentity, now time.Time) error {
	if err := lockBackgroundSessionTx(ctx, tx, identity.WorkspaceID, identity.SessionID); err != nil {
		return err
	}
	var status string
	var generation int64
	var releaseOperationID sql.NullString
	err := tx.QueryRow(ctx, `SELECT status, reconcile_generation, release_operation_id
		FROM session_background_tasks
		WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3 FOR UPDATE`,
		identity.WorkspaceID, identity.SessionID, identity.TaskID,
	).Scan(&status, &generation, &releaseOperationID)
	if dbconnect.IsNoRows(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if status != "running" || generation != identity.Generation || releaseOperationID.Valid {
		return nil
	}
	nextGeneration := generation + 1
	nextPoll := now.Add(SandboxBackgroundOutageRetryBackoff)
	result, err := tx.Exec(ctx, `UPDATE session_background_tasks
		SET reconcile_generation=$4, next_poll_at=$5, updated_at=$6
		WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3
		  AND status='running' AND reconcile_generation=$7 AND release_operation_id IS NULL`,
		identity.WorkspaceID, identity.SessionID, identity.TaskID, nextGeneration, nextPoll, now, generation)
	if err != nil {
		return err
	}
	if !transitionRowsAffected(result) {
		return nil
	}
	return enqueueBackgroundReconcileTx(ctx, tx, identity.WorkspaceID, identity.SessionID, identity.TaskID, nextGeneration, now, nextPoll)
}

func finalizeExhaustedBackgroundCommandTx(ctx context.Context, tx *dbconnect.Tx, identity backgroundQueueIdentity, now time.Time) error {
	if err := lockBackgroundSessionTx(ctx, tx, identity.WorkspaceID, identity.SessionID); err != nil {
		return err
	}
	work, current, err := loadBackgroundCommandForUpdateTx(ctx, tx, SandboxBackgroundCommandJob{
		WorkspaceID: identity.WorkspaceID, SessionID: identity.SessionID, TaskID: identity.TaskID, RequestID: identity.RequestID,
	}, now)
	if err != nil || !current {
		return err
	}
	if work.Kind == SandboxBackgroundOperationCancel && work.Task.ReleaseOperationID != "" && work.State == SandboxBackgroundOperationPending {
		var nextGeneration int64
		if err := tx.QueryRow(ctx,
			`UPDATE session_background_tasks
			    SET reconcile_generation=reconcile_generation+1, updated_at=$4
			  WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3
			    AND status='running' AND release_operation_id=$5
			  RETURNING reconcile_generation`,
			identity.WorkspaceID, identity.SessionID, identity.TaskID, now, work.Task.ReleaseOperationID,
		).Scan(&nextGeneration); dbconnect.IsNoRows(err) {
			return nil
		} else if err != nil {
			return err
		}
		nextRequestID := releaseBackgroundCancelRequestID(work.Task.ReleaseOperationID, identity.TaskID, nextGeneration)
		exhausted := normalizeSandboxProviderResult(backgroundTaskFailure("sandbox_background_unavailable", "sandbox background command is unavailable").ResultJSON)
		digest := sha256.Sum256([]byte(exhausted))
		if _, err := tx.Exec(ctx, `UPDATE session_runtime_tool_results
				SET background_operation_state='terminal', result_json=$6, result_digest=$7, updated_at=$8
				WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
				  AND tool_use_event_id=$4 AND background_request_id=$5 AND background_operation_state='pending'`,
			identity.WorkspaceID, identity.SessionID, work.Task.SessionThreadID, work.ToolUseEventID,
			identity.RequestID, exhausted, hex.EncodeToString(digest[:]), now); err != nil {
			return err
		}
		inputDigest := sha256.Sum256([]byte(work.InputJSON))
		if _, err := tx.Exec(ctx, `INSERT INTO session_runtime_tool_results (
				workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
				normalized_input_hash, tool_name, input_json, ack_status,
				background_operation_kind, background_operation_state, background_request_id,
				background_task_id, background_max_output_tokens, created_at, updated_at
			) VALUES ($1,$2,$3,$4,'sandbox_background',$5,'cancel_command',$6,'committed',
				'cancel','pending',$7,$8,0,$9,$9)`, identity.WorkspaceID, identity.SessionID,
			work.Task.SessionThreadID, backgroundOperationReceiptID(nextRequestID),
			hex.EncodeToString(inputDigest[:]), work.InputJSON, nextRequestID, identity.TaskID, now); err != nil {
			return err
		}
		request, err := releaseBackgroundCancelEnqueueRequest(identity.WorkspaceID, identity.SessionID, identity.TaskID, nextRequestID, now)
		if err != nil {
			return err
		}
		_, err = queue.EnqueueBatchTx(ctx, tx, []queue.EnqueueRequest{request})
		return err
	}
	disposition := "failed"
	result := backgroundTaskFailure("sandbox_background_unavailable", "sandbox background command is unavailable")
	result.TerminalStatus = ""
	if work.Kind == SandboxBackgroundOperationPoll {
		result = backgroundTaskFailure("background_observation_unavailable", "background task observation is unavailable")
		result.TerminalStatus = ""
	} else if work.State == SandboxBackgroundOperationSubmitted {
		disposition = "unknown_outcome"
		result = backgroundTaskFailure("sandbox_background_outcome_unknown", "background command outcome is unknown")
		result.TerminalStatus = ""
	}
	return settleBackgroundCommandTx(ctx, tx, work, disposition, result, now)
}

func loadBackgroundCommandForUpdateTx(ctx context.Context, tx *dbconnect.Tx, job SandboxBackgroundCommandJob, now time.Time) (SandboxBackgroundOperationWork, bool, error) {
	var threadID, toolUseEventID, operationKind, operationState, backgroundRequestID, taskID, inputJSON string
	var maxOutputTokens int
	err := tx.QueryRow(ctx, `SELECT session_thread_id, tool_use_event_id,
		background_operation_kind, background_operation_state, background_request_id,
		background_task_id, input_json, background_max_output_tokens
		FROM session_runtime_tool_results
		WHERE workspace_id=$1 AND session_id=$2 AND tool_kind='sandbox_background'
		  AND background_task_id=$3
		  AND background_request_id=$4
		FOR UPDATE`, job.WorkspaceID, job.SessionID, job.TaskID, job.RequestID).Scan(
		&threadID, &toolUseEventID, &operationKind, &operationState, &backgroundRequestID,
		&taskID, &inputJSON, &maxOutputTokens,
	)
	if dbconnect.IsNoRows(err) {
		return SandboxBackgroundOperationWork{}, false, nil
	}
	if err != nil {
		return SandboxBackgroundOperationWork{}, false, err
	}
	task, running, err := loadBackgroundTaskForUpdateTx(ctx, tx, job.WorkspaceID, job.SessionID, taskID)
	if err != nil {
		return SandboxBackgroundOperationWork{}, false, err
	}
	if !running {
		var terminalResult string
		if err := tx.QueryRow(ctx, `SELECT terminal_result_json FROM session_background_tasks
			WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3`, job.WorkspaceID, job.SessionID, taskID).Scan(&terminalResult); err != nil {
			return SandboxBackgroundOperationWork{}, false, err
		}
		digestBytes := sha256.Sum256([]byte(terminalResult))
		_, err = tx.Exec(ctx, `UPDATE session_runtime_tool_results
			SET background_operation_state='terminal', result_json=$5, result_digest=$6, updated_at=$7
			WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4`,
			job.WorkspaceID, job.SessionID, threadID, toolUseEventID, terminalResult, hex.EncodeToString(digestBytes[:]), now)
		return SandboxBackgroundOperationWork{}, false, err
	}
	work := SandboxBackgroundOperationWork{
		Task: task, RequestID: job.RequestID, ToolUseEventID: toolUseEventID,
		Kind: SandboxBackgroundOperationKind(operationKind), State: operationState,
		InputJSON: inputJSON, MaxOutputTokens: maxOutputTokens,
	}
	return work, true, nil
}

func settleBackgroundCommandTx(ctx context.Context, tx *dbconnect.Tx, work SandboxBackgroundOperationWork, disposition string, result sandboxdriver.CommandResult, now time.Time) error {
	if work.Kind == SandboxBackgroundOperationCancel && work.Task.ReleaseOperationID != "" && disposition != "completed" {
		result.TerminalStatus = "unknown_outcome"
	}
	if disposition == "completed" && result.TerminalStatus != "" {
		if err := settleBackgroundTaskResultTx(ctx, tx, work.Task, result, now); err != nil {
			return err
		}
		var winning string
		if err := tx.QueryRow(ctx, `SELECT terminal_result_json FROM session_background_tasks
			WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3`, work.Task.WorkspaceID, work.Task.SessionID, work.Task.TaskID).Scan(&winning); err != nil {
			return err
		}
		result.ResultJSON = winning
	} else if work.Kind == SandboxBackgroundOperationCancel && work.Task.ReleaseOperationID != "" && result.TerminalStatus != "" {
		if err := settleBackgroundTaskResultTx(ctx, tx, work.Task, result, now); err != nil {
			return err
		}
	}
	digestBytes := sha256.Sum256([]byte(result.ResultJSON))
	_, err := tx.Exec(ctx, `UPDATE session_runtime_tool_results
		SET background_operation_state='terminal', result_json=$6, result_digest=$7, updated_at=$8
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
		  AND tool_use_event_id=$4 AND background_request_id=$5
		  AND background_operation_state IN ('pending','submitted')`, work.Task.WorkspaceID, work.Task.SessionID,
		work.Task.SessionThreadID, work.ToolUseEventID, work.RequestID,
		result.ResultJSON, hex.EncodeToString(digestBytes[:]), now)
	return err
}

func backgroundOperationReceiptID(requestID string) string {
	return "background_receipt:" + requestID
}

func (s *PostgreSQLSandboxBackgroundCommandStore) withWorkspaceTx(ctx context.Context, workspaceID string, operation string, fn func(*dbconnect.Tx) error) error {
	if s == nil || s.client == nil {
		return errors.New("sandbox background store database is required")
	}
	return s.client.WithWorkspaceTx(ctx, workspaceID, operation, fn)
}

func loadBackgroundTaskForUpdateTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, taskID string) (SandboxBackgroundTaskWork, bool, error) {
	var work SandboxBackgroundTaskWork
	var sourceToolUseEventID, providerResourceID, providerCommandID, providerMetadataJSON, resourceRootsJSON string
	var releaseOperationID sql.NullString
	var status string
	err := tx.QueryRow(ctx, `SELECT session_thread_id, source_tool_use_event_id, provider, binding_revision,
		provider_session_id, provider_command_id, provider_command_metadata_json, resource_roots_json,
		status, reconcile_generation, release_operation_id
		FROM session_background_tasks WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3 FOR UPDATE`,
		workspaceID, sessionID, taskID,
	).Scan(&work.SessionThreadID, &sourceToolUseEventID, &work.Provider, &work.Reference.Target.BindingGeneration,
		&providerResourceID, &providerCommandID, &providerMetadataJSON, &resourceRootsJSON, &status, &work.ReconcileGeneration, &releaseOperationID)
	if dbconnect.IsNoRows(err) {
		return SandboxBackgroundTaskWork{}, false, nil
	}
	if err != nil {
		return SandboxBackgroundTaskWork{}, false, err
	}
	work.WorkspaceID = workspaceID
	work.SessionID = sessionID
	work.TaskID = taskID
	work.ReleaseOperationID = releaseOperationID.String
	work.Reference = sandboxdriver.CommandReference{
		Target: sandboxdriver.ToolTarget{
			WorkspaceID: workspaceID, SessionID: sessionID, SessionThreadID: work.SessionThreadID,
			BindingGeneration: work.Reference.Target.BindingGeneration, ProviderSandboxID: providerResourceID,
			ResourceRootsJSON: resourceRootsJSON,
		},
		Task: sandboxdriver.BackgroundTask{
			TaskID: taskID, SourceToolUseEventID: sourceToolUseEventID,
			ProviderSessionID: providerResourceID, ProviderCommandID: providerCommandID,
			ProviderCommandMetadataJSON: providerMetadataJSON,
		},
	}
	return work, status == "running", nil
}

func settleBackgroundTaskResultTx(ctx context.Context, tx *dbconnect.Tx, work SandboxBackgroundTaskWork, result sandboxdriver.CommandResult, now time.Time) error {
	status := normalizeSandboxBackgroundTerminalStatus(result.TerminalStatus)
	if status == "" {
		return errors.New("background task terminal status is invalid")
	}
	digestBytes := sha256.Sum256([]byte(result.ResultJSON))
	digest := hex.EncodeToString(digestBytes[:])
	updated, err := tx.Exec(ctx, `UPDATE session_background_tasks
		SET status=$4, terminal_result_json=$5, terminal_result_digest=$6,
		    terminal_at=COALESCE(terminal_at,$7), next_poll_at=NULL,
		    reconcile_generation=reconcile_generation+1, updated_at=$7
		WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3 AND status='running'`,
		work.WorkspaceID, work.SessionID, work.TaskID, status, result.ResultJSON, digest, now)
	if err != nil {
		return err
	}
	if !transitionRowsAffected(updated) {
		return nil
	}
	runtimeInputID := "task_notification:" + work.TaskID
	if _, err := tx.Exec(ctx, `INSERT INTO session_runtime_inbox (
		workspace_id, session_id, session_thread_id, runtime_input_id, input_kind,
		event_ids_json, status,
		binding_id, binding_generation, target_pod_uid, created_at, updated_at
	) VALUES ($1,$2,$3,$4,'task_notification','[]','queued',NULL,NULL,NULL,$5,$5)
	ON CONFLICT (workspace_id, runtime_input_id) DO NOTHING`,
		work.WorkspaceID, work.SessionID, work.SessionThreadID, runtimeInputID, now); err != nil {
		return err
	}
	request, err := queue.NewTaskNotificationRuntimeInputEnqueueRequest(
		workspace.ID(work.WorkspaceID), work.SessionID, work.SessionThreadID, work.TaskID, now,
	)
	if err != nil {
		return err
	}
	if _, err = queue.EnqueueTx(ctx, tx, request); err != nil {
		return err
	}
	return enqueueReadySandboxReleasesTx(ctx, tx, work.WorkspaceID, work.SessionID, now)
}

func enqueueBackgroundReconcileTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, taskID string, generation int64, now time.Time, availableAt time.Time) error {
	payload, err := json.Marshal(struct {
		WorkspaceID         string `json:"workspace_id"`
		SessionID           string `json:"session_id"`
		TaskID              string `json:"task_id"`
		ReconcileGeneration int64  `json:"reconcile_generation"`
	}{workspaceID, sessionID, taskID, generation})
	if err != nil {
		return err
	}
	ws := workspace.ID(workspaceID)
	_, err = queue.EnqueueTx(ctx, tx, queue.EnqueueRequest{
		ID: queue.NewJobID(), WorkspaceID: ws, Kind: queue.KindSandboxBackgroundReconcile,
		PartitionKey:   queue.FormatSandboxBackgroundPartitionKey(ws, sessionID, taskID),
		DedupeKey:      queue.FormatSandboxBackgroundReconcileDedupeKey(ws, sessionID, taskID, generation),
		PayloadVersion: 1, PayloadJSON: payload, MaxAttempts: SandboxBackgroundReconcileMaxAttempts,
		AvailableAt: availableAt, Now: now,
	})
	return err
}

func lockBackgroundSessionTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) error {
	var locked string
	err := tx.QueryRow(ctx, `SELECT id FROM sessions WHERE workspace_id=$1 AND id=$2 FOR UPDATE`, workspaceID, sessionID).Scan(&locked)
	if dbconnect.IsNoRows(err) {
		return sql.ErrNoRows
	}
	return err
}

func normalizeSandboxBackgroundTerminalStatus(status string) string {
	switch status {
	case "completed", "failed", "cancelled", "expired", "unknown_outcome":
		return status
	default:
		return ""
	}
}

func normalizeSandboxProviderResult(raw string) string {
	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return raw
	}
	value = stripSandboxProviderFields(value)
	encoded, err := json.Marshal(value)
	if err != nil {
		return raw
	}
	return string(encoded)
}

func stripSandboxProviderFields(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		for _, key := range []string{
			"background_task", "engine_sandbox_id", "provider_sandbox_id",
			"provider_session_id", "provider_command_id", "provider_command_metadata",
			"provider_command_metadata_json", "provider_metadata", "provider_metadata_json",
		} {
			delete(typed, key)
		}
		for key, child := range typed {
			typed[key] = stripSandboxProviderFields(child)
		}
		return typed
	case []any:
		for index, child := range typed {
			typed[index] = stripSandboxProviderFields(child)
		}
		return typed
	default:
		return value
	}
}
