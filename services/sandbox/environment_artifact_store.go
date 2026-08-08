package tetralsandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type EnvironmentArtifactStore struct {
	client *dbconnect.Client
}

type EnvironmentArtifactBuildInput struct {
	WorkspaceID        workspace.ID
	EnvironmentID      string
	Generation         int64
	Provider           string
	ArtifactInputHash  string
	NormalizedPackages sandbox.PackageSetup
}

type EnvironmentArtifactFailure struct {
	Stage         string
	LastErrorKind string
	Reason        string
	Retryable     bool
}

func NewEnvironmentArtifactStore(client *dbconnect.Client) *EnvironmentArtifactStore {
	return &EnvironmentArtifactStore{client: client}
}

func (s *EnvironmentArtifactStore) ClaimEnvironmentBuild(ctx context.Context, job EnvironmentBuildJob, now time.Time) (EnvironmentArtifactBuildInput, bool, error) {
	if s == nil || s.client == nil {
		return EnvironmentArtifactBuildInput{}, false, errors.New("environment artifact store is required")
	}
	if now.IsZero() {
		now = storage.Now()
	}
	var input EnvironmentArtifactBuildInput
	var claimed bool
	err := s.client.WithWorkspaceTx(ctx, job.WorkspaceID, "sandbox.environment_build.claim", func(tx *dbconnect.Tx) (txErr error) {
		defer finishSandboxQueueAuthorityTx(ctx, tx, &txErr)
		var (
			status            string
			provider          string
			packagesJSON      string
			artifactInputHash string
			leaseJobID        sql.NullString
			leaseToken        sql.NullString
			leaseAttemptCount sql.NullInt64
		)
		err := tx.QueryRow(ctx,
			`SELECT status, provider, packages_json, artifact_input_hash,
			        lease_job_id, lease_token, lease_attempt_count
			   FROM environment_artifacts
			  WHERE workspace_id = $1
			    AND environment_id = $2
			    AND generation = $3
			  FOR UPDATE`,
			job.WorkspaceID, job.EnvironmentID, job.Generation,
		).Scan(&status, &provider, &packagesJSON, &artifactInputHash, &leaseJobID, &leaseToken, &leaseAttemptCount)
		if dbconnect.IsNoRows(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if status == "ready" || status == "failed" {
			return nil
		}
		if job.JobID == "" || job.LeaseToken == "" || job.AttemptCount <= 0 {
			return errors.New("environment build lease identity is incomplete")
		}
		if status == "building" && leaseAttemptCount.Valid {
			if int64(job.AttemptCount) < leaseAttemptCount.Int64 {
				return nil
			}
			if int64(job.AttemptCount) == leaseAttemptCount.Int64 &&
				(!leaseJobID.Valid || leaseJobID.String != job.JobID || !leaseToken.Valid || leaseToken.String != job.LeaseToken) {
				return nil
			}
		}
		packages, err := decodePackageSetupJSON(packagesJSON)
		if err != nil {
			return err
		}
		result, err := tx.Exec(ctx,
			`UPDATE environment_artifacts
			    SET status = 'building',
			        lease_job_id = $4,
			        lease_token = $5,
			        lease_attempt_count = $6,
			        provider_artifact_ref = NULL,
			        failure_stage = NULL,
			        last_error_kind = NULL,
			        failure_reason = NULL,
			        retryable = NULL,
			        updated_at = $7
			  WHERE workspace_id = $1
			    AND environment_id = $2
			    AND generation = $3
			    AND status IN ('pending', 'building')`,
			job.WorkspaceID, job.EnvironmentID, job.Generation, job.JobID, job.LeaseToken, job.AttemptCount, now.UTC(),
		)
		if err != nil {
			return err
		}
		if !transitionRowsAffected(result) {
			return nil
		}
		input = EnvironmentArtifactBuildInput{
			WorkspaceID:        workspace.ID(job.WorkspaceID),
			EnvironmentID:      job.EnvironmentID,
			Generation:         job.Generation,
			Provider:           provider,
			ArtifactInputHash:  artifactInputHash,
			NormalizedPackages: packages,
		}
		claimed = true
		return nil
	})
	return input, claimed, err
}

func (s *EnvironmentArtifactStore) AuthorizeEnvironmentArtifactCreate(ctx context.Context, job EnvironmentBuildJob, now time.Time) (bool, error) {
	if s == nil || s.client == nil {
		return false, errors.New("environment artifact store is required")
	}
	if now.IsZero() {
		now = storage.Now()
	}
	var authorized bool
	err := s.client.WithWorkspaceTx(ctx, job.WorkspaceID, "sandbox.environment_build.authorize_create", func(tx *dbconnect.Tx) (txErr error) {
		defer finishSandboxQueueAuthorityTx(ctx, tx, &txErr)
		var status string
		var leaseJobID, leaseToken sql.NullString
		var leaseAttemptCount sql.NullInt64
		if err := tx.QueryRow(ctx,
			`SELECT status, lease_job_id, lease_token, lease_attempt_count FROM environment_artifacts
			  WHERE workspace_id = $1 AND environment_id = $2 AND generation = $3
			  FOR UPDATE`,
			job.WorkspaceID, job.EnvironmentID, job.Generation,
		).Scan(&status, &leaseJobID, &leaseToken, &leaseAttemptCount); dbconnect.IsNoRows(err) {
			return nil
		} else if err != nil {
			return err
		}
		if status != "building" {
			return nil
		}
		if !environmentBuildLeaseMatches(job, leaseJobID, leaseToken, leaseAttemptCount) {
			return nil
		}
		result, err := tx.Exec(ctx,
			`UPDATE environment_artifacts
			    SET provider_create_submitted_at = $4,
			        updated_at = $4
			  WHERE workspace_id = $1
			    AND environment_id = $2
			    AND generation = $3
			    AND status = 'building'
			    AND provider_create_submitted_at IS NULL`,
			job.WorkspaceID, job.EnvironmentID, job.Generation, now.UTC(),
		)
		if err != nil {
			return err
		}
		authorized = transitionRowsAffected(result)
		return nil
	})
	return authorized, err
}

func (s *EnvironmentArtifactStore) MarkEnvironmentBuildReady(ctx context.Context, job EnvironmentBuildJob, providerArtifactRef string, now time.Time) error {
	if s == nil || s.client == nil {
		return errors.New("environment artifact store is required")
	}
	if providerArtifactRef == "" {
		return errors.New("provider artifact ref is required")
	}
	if now.IsZero() {
		now = storage.Now()
	}
	return s.client.WithWorkspaceTx(ctx, job.WorkspaceID, "sandbox.environment_build.ready", func(tx *dbconnect.Tx) (txErr error) {
		defer finishSandboxQueueAuthorityTx(ctx, tx, &txErr)
		var status string
		var artifactInputHash string
		var leaseJobID, leaseToken sql.NullString
		var leaseAttemptCount sql.NullInt64
		err := tx.QueryRow(ctx,
			`SELECT status, artifact_input_hash, lease_job_id, lease_token, lease_attempt_count
			   FROM environment_artifacts
			  WHERE workspace_id = $1
			    AND environment_id = $2
			    AND generation = $3
			  FOR UPDATE`,
			job.WorkspaceID, job.EnvironmentID, job.Generation,
		).Scan(&status, &artifactInputHash, &leaseJobID, &leaseToken, &leaseAttemptCount)
		if dbconnect.IsNoRows(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if !environmentBuildLeaseMatches(job, leaseJobID, leaseToken, leaseAttemptCount) {
			return nil
		}
		generations := []int64{job.Generation}
		if status != "ready" {
			if status != "building" {
				return errors.New("environment artifact is not building")
			}
			rows, err := tx.Query(ctx,
				`UPDATE environment_artifacts
				    SET status = 'ready',
				        provider_artifact_ref = $4,
				        lease_job_id = NULL,
				        lease_token = NULL,
				        lease_attempt_count = NULL,
				        failure_stage = NULL,
				        last_error_kind = NULL,
				        failure_reason = NULL,
				        retryable = NULL,
				        updated_at = $5
			  WHERE workspace_id = $1
			    AND environment_id = $2
			    AND artifact_input_hash = $3
			    AND status IN ('pending', 'building')
			RETURNING generation`,
				job.WorkspaceID, job.EnvironmentID, artifactInputHash, providerArtifactRef, now.UTC(),
			)
			if err != nil {
				return err
			}
			defer func() { _ = rows.Close() }()
			generations = generations[:0]
			for rows.Next() {
				var generation int64
				if err := rows.Scan(&generation); err != nil {
					return err
				}
				generations = append(generations, generation)
			}
			if err := rows.Err(); err != nil {
				return err
			}
			if len(generations) == 0 {
				return errors.New("environment artifact is not building")
			}
		}
		for _, generation := range generations {
			if err := enqueueEnvironmentReadyFanout(ctx, tx, EnvironmentBuildJob{
				WorkspaceID: job.WorkspaceID, EnvironmentID: job.EnvironmentID, Generation: generation,
			}, now.UTC()); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *EnvironmentArtifactStore) MarkEnvironmentBuildRetryableFailure(ctx context.Context, job EnvironmentBuildJob, failure EnvironmentArtifactFailure, rearmCreate bool, now time.Time) error {
	if s == nil || s.client == nil {
		return errors.New("environment artifact store is required")
	}
	if now.IsZero() {
		now = storage.Now()
	}
	return s.client.WithWorkspaceTx(ctx, job.WorkspaceID, "sandbox.environment_build.retryable_failure", func(tx *dbconnect.Tx) (txErr error) {
		defer finishSandboxQueueAuthorityTx(ctx, tx, &txErr)
		var status string
		var leaseJobID, leaseToken sql.NullString
		var leaseAttemptCount sql.NullInt64
		if err := tx.QueryRow(ctx,
			`SELECT status, lease_job_id, lease_token, lease_attempt_count FROM environment_artifacts
			  WHERE workspace_id = $1 AND environment_id = $2 AND generation = $3
			  FOR UPDATE`,
			job.WorkspaceID, job.EnvironmentID, job.Generation,
		).Scan(&status, &leaseJobID, &leaseToken, &leaseAttemptCount); dbconnect.IsNoRows(err) {
			return nil
		} else if err != nil {
			return err
		}
		if status != "building" {
			return nil
		}
		if !environmentBuildLeaseMatches(job, leaseJobID, leaseToken, leaseAttemptCount) {
			return nil
		}
		_, err := tx.Exec(ctx,
			`UPDATE environment_artifacts
			    SET status = 'pending',
			        lease_job_id = NULL,
			        lease_token = NULL,
			        lease_attempt_count = NULL,
			        provider_create_submitted_at = CASE WHEN $7 THEN NULL ELSE provider_create_submitted_at END,
			        failure_stage = $4,
			        last_error_kind = $5,
			        failure_reason = $6,
			        retryable = TRUE,
			        updated_at = $8
			  WHERE workspace_id = $1
			    AND environment_id = $2
			    AND generation = $3
			    AND status = 'building'`,
			job.WorkspaceID, job.EnvironmentID, job.Generation,
			nullIfEmpty(failure.Stage), nullIfEmpty(failure.LastErrorKind), nullIfEmpty(failure.Reason), rearmCreate, now.UTC(),
		)
		return err
	})
}

func (s *EnvironmentArtifactStore) MarkEnvironmentBuildTerminalFailure(ctx context.Context, job EnvironmentBuildJob, failure EnvironmentArtifactFailure, now time.Time) error {
	if s == nil || s.client == nil {
		return errors.New("environment artifact store is required")
	}
	if now.IsZero() {
		now = storage.Now()
	}
	return s.client.WithWorkspaceTx(ctx, job.WorkspaceID, "sandbox.environment_build.terminal_failure", func(tx *dbconnect.Tx) (txErr error) {
		defer finishSandboxQueueAuthorityTx(ctx, tx, &txErr)
		timestamp := now.UTC()
		var artifactInputHash string
		var leaseJobID, leaseToken sql.NullString
		var leaseAttemptCount sql.NullInt64
		if err := tx.QueryRow(ctx,
			`SELECT artifact_input_hash, lease_job_id, lease_token, lease_attempt_count
			   FROM environment_artifacts
			  WHERE workspace_id = $1 AND environment_id = $2 AND generation = $3
			  FOR UPDATE`,
			job.WorkspaceID, job.EnvironmentID, job.Generation,
		).Scan(&artifactInputHash, &leaseJobID, &leaseToken, &leaseAttemptCount); dbconnect.IsNoRows(err) {
			return nil
		} else if err != nil {
			return err
		}
		if !environmentBuildLeaseMatches(job, leaseJobID, leaseToken, leaseAttemptCount) {
			return nil
		}
		rows, err := tx.Query(ctx,
			`UPDATE environment_artifacts
			    SET status = 'failed',
			        lease_job_id = NULL,
			        lease_token = NULL,
			        lease_attempt_count = NULL,
			        failure_stage = $4,
			        last_error_kind = $5,
			        failure_reason = $6,
			        retryable = FALSE,
			        updated_at = $7
			  WHERE workspace_id = $1
			    AND environment_id = $2
			    AND artifact_input_hash = $3
			    AND status IN ('pending', 'building')
			RETURNING generation`,
			job.WorkspaceID, job.EnvironmentID, artifactInputHash,
			nullIfEmpty(failure.Stage), nullIfEmpty(failure.LastErrorKind), nullIfEmpty(failure.Reason), timestamp,
		)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		var generations []int64
		for rows.Next() {
			var generation int64
			if err := rows.Scan(&generation); err != nil {
				return err
			}
			generations = append(generations, generation)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(generations) == 0 {
			return nil
		}
		return failWaitingArtifactActivationsTx(ctx, tx, job.WorkspaceID, job.EnvironmentID, generations, failure, timestamp)
	})
}

func environmentBuildLeaseMatches(job EnvironmentBuildJob, leaseJobID sql.NullString, leaseToken sql.NullString, leaseAttemptCount sql.NullInt64) bool {
	return job.JobID != "" && job.LeaseToken != "" &&
		leaseJobID.Valid && leaseJobID.String == job.JobID &&
		leaseToken.Valid && leaseToken.String == job.LeaseToken &&
		leaseAttemptCount.Valid && leaseAttemptCount.Int64 == int64(job.AttemptCount)
}

func (s *EnvironmentArtifactStore) FanoutReadyEnvironment(ctx context.Context, job EnvironmentReadyFanoutJob, now time.Time) (int, error) {
	if s == nil || s.client == nil {
		return 0, errors.New("environment artifact store is required")
	}
	if now.IsZero() {
		now = storage.Now()
	}
	var advanced int
	err := s.client.WithWorkspaceTx(ctx, job.WorkspaceID, "sandbox.environment_ready_fanout", func(tx *dbconnect.Tx) (txErr error) {
		defer finishSandboxQueueAuthorityTx(ctx, tx, &txErr)
		var status string
		err := tx.QueryRow(ctx,
			`SELECT status
			   FROM environment_artifacts
			  WHERE workspace_id = $1
			    AND environment_id = $2
			    AND generation = $3
			  FOR UPDATE`,
			job.WorkspaceID, job.EnvironmentID, job.Generation,
		).Scan(&status)
		if dbconnect.IsNoRows(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if status != "ready" {
			return errors.New("environment artifact is not ready for fanout")
		}
		activationCount, err := requeueWaitingArtifactActivationsTx(ctx, tx, job, now.UTC())
		if err != nil {
			return err
		}
		advanced = activationCount
		return nil
	})
	return advanced, err
}

func requeueWaitingArtifactActivationsTx(ctx context.Context, tx *dbconnect.Tx, job EnvironmentReadyFanoutJob, now time.Time) (int, error) {
	rows, err := tx.Query(ctx,
		`SELECT o.operation_id, o.session_id, o.logical_sandbox_id
		   FROM sandbox_lifecycle_operations o
		   JOIN sessions s
		     ON s.workspace_id = o.workspace_id AND s.id = o.session_id
		  WHERE o.workspace_id = $1
		    AND s.environment_id = $2
		    AND o.target_environment_generation = $3
		    AND o.kind IN ('create', 'replace')
		    AND o.state = 'waiting_artifact'
		  ORDER BY o.operation_id
		  FOR UPDATE OF o`,
		job.WorkspaceID, job.EnvironmentID, job.Generation,
	)
	if err != nil {
		return 0, err
	}
	type activationRef struct {
		operationID      string
		sessionID        string
		logicalSandboxID string
	}
	var activations []activationRef
	for rows.Next() {
		var ref activationRef
		if err := rows.Scan(&ref.operationID, &ref.sessionID, &ref.logicalSandboxID); err != nil {
			_ = rows.Close()
			return 0, err
		}
		activations = append(activations, ref)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, err
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	for _, ref := range activations {
		queueJobID := queue.NewJobID()
		partitionKey := queue.FormatSandboxLifecyclePartitionKey(workspace.ID(job.WorkspaceID), ref.logicalSandboxID)
		dedupeKey := queue.FormatSandboxLifecycleDedupeKey(queue.KindSandboxActivate, workspace.ID(job.WorkspaceID), ref.logicalSandboxID, ref.operationID)
		if _, err := tx.Exec(ctx,
			`UPDATE sandbox_lifecycle_operations
			    SET state = 'pending', queue_job_id = $3, queue_kind = $4,
			        queue_partition_key = $5, queue_dedupe_key = $6,
			        lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
			        attempt_count = 0,
			        updated_at = $7
			  WHERE workspace_id = $1 AND operation_id = $2 AND state = 'waiting_artifact'`,
			job.WorkspaceID, ref.operationID, queueJobID, queue.KindSandboxActivate,
			partitionKey, dedupeKey, now,
		); err != nil {
			return 0, err
		}
		payload, err := sandboxLifecycleQueuePayload(SandboxExecutionRef{
			WorkspaceID: job.WorkspaceID, SessionID: ref.sessionID,
		}, ref.logicalSandboxID, ref.operationID)
		if err != nil {
			return 0, err
		}
		enqueued, err := queue.EnqueueTx(ctx, tx, queue.EnqueueRequest{
			ID: queueJobID, WorkspaceID: workspace.ID(job.WorkspaceID), Kind: queue.KindSandboxActivate,
			PartitionKey: partitionKey, DedupeKey: dedupeKey, PayloadVersion: 1,
			PayloadJSON: payload, MaxAttempts: sandboxActivationMaxAttempts, Now: now,
		})
		if err != nil {
			return 0, err
		}
		if enqueued.ID != queueJobID {
			return 0, errors.New("sandbox activation predecessor notification is still active")
		}
	}
	return len(activations), nil
}

func (s *EnvironmentArtifactStore) FinalizeReadyEnvironmentFanout(ctx context.Context, job EnvironmentReadyFanoutJob, now time.Time) error {
	if s == nil || s.client == nil {
		return errors.New("environment artifact store is required")
	}
	if now.IsZero() {
		now = storage.Now()
	}
	return s.client.WithWorkspaceTx(ctx, job.WorkspaceID, "sandbox.environment_ready_fanout.finalize", func(tx *dbconnect.Tx) (txErr error) {
		defer finishSandboxQueueAuthorityTx(ctx, tx, &txErr)
		var status string
		if err := tx.QueryRow(ctx,
			`SELECT status FROM environment_artifacts
			  WHERE workspace_id=$1 AND environment_id=$2 AND generation=$3
			  FOR UPDATE`,
			job.WorkspaceID, job.EnvironmentID, job.Generation,
		).Scan(&status); err != nil {
			return err
		}
		if status != "ready" {
			return errors.New("environment artifact is not ready for fanout finalization")
		}
		return failWaitingArtifactActivationsTx(ctx, tx, job.WorkspaceID, job.EnvironmentID, []int64{job.Generation}, EnvironmentArtifactFailure{
			Stage: "environment_ready_fanout", LastErrorKind: "environment_ready_fanout_failed",
			Reason: "sandbox environment could not resume waiting tools", Retryable: false,
		}, now.UTC())
	})
}

func failWaitingArtifactActivationsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, environmentID string, generations []int64, failure EnvironmentArtifactFailure, now time.Time) error {
	rows, err := tx.Query(ctx,
		`SELECT o.operation_id
		   FROM sandbox_lifecycle_operations o
		   JOIN sessions s
		     ON s.workspace_id = o.workspace_id AND s.id = o.session_id
		  WHERE o.workspace_id = $1
		    AND s.environment_id = $2
		    AND o.target_environment_generation = ANY($3)
		    AND o.kind IN ('create', 'replace')
		    AND o.state = 'waiting_artifact'
		  ORDER BY o.operation_id
		  FOR UPDATE OF o`,
		workspaceID, environmentID, generations,
	)
	if err != nil {
		return err
	}
	var operationIDs []string
	for rows.Next() {
		var operationID string
		if err := rows.Scan(&operationID); err != nil {
			_ = rows.Close()
			return err
		}
		operationIDs = append(operationIDs, operationID)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	kind := failure.LastErrorKind
	if kind == "" {
		kind = "environment_artifact_failed"
	}
	message := failure.Reason
	if message == "" {
		message = "sandbox environment artifact is unavailable"
	}
	for _, operationID := range operationIDs {
		if _, err := tx.Exec(ctx,
			`UPDATE sandbox_lifecycle_operations
			    SET state = 'failed', outcome_effect_boundary = 'proved_not_started',
			        outcome_disposition = 'terminal', error_kind = $3, safe_message = $4,
			        completed_at = $5, updated_at = $5
			  WHERE workspace_id = $1 AND operation_id = $2 AND state = 'waiting_artifact'`,
			workspaceID, operationID, kind, message, now,
		); err != nil {
			return err
		}
		waiters, err := lockExecutionWaiters(ctx, tx, workspaceID, operationID, "waiting_activation_operation_id")
		if err != nil {
			return err
		}
		if err := settleExecutionWaitersTx(ctx, tx, waiters, kind, message, now); err != nil {
			return err
		}
		if err := failActivationMaterializationDependentsTx(ctx, tx, workspaceID, operationID, kind, message, now); err != nil {
			return err
		}
	}
	return nil
}

func enqueueEnvironmentReadyFanout(ctx context.Context, tx *dbconnect.Tx, job EnvironmentBuildJob, now time.Time) error {
	payload, err := json.Marshal(map[string]string{
		"workspace_id":   job.WorkspaceID,
		"environment_id": job.EnvironmentID,
		"generation":     strconv.FormatInt(job.Generation, 10),
	})
	if err != nil {
		return err
	}
	ws := workspace.ID(job.WorkspaceID)
	_, err = queue.EnqueueTx(ctx, tx, queue.EnqueueRequest{
		WorkspaceID:    ws,
		Kind:           queue.KindEnvironmentReadyFanout,
		PartitionKey:   queue.FormatEnvironmentPartitionKey(ws, job.EnvironmentID),
		DedupeKey:      queue.FormatEnvironmentReadyFanoutDedupeKey(ws, job.EnvironmentID, strconv.FormatInt(job.Generation, 10)),
		PayloadVersion: 1,
		PayloadJSON:    payload,
		Now:            now,
	})
	return err
}

func decodePackageSetupJSON(raw string) (sandbox.PackageSetup, error) {
	var packages map[string][]string
	if err := json.Unmarshal([]byte(raw), &packages); err != nil {
		return nil, err
	}
	setup := sandbox.PackageSetup{}
	for manager, entries := range packages {
		setup[manager] = append([]string(nil), entries...)
	}
	return setup, nil
}

func nullIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func transitionRowsAffected(result interface {
	RowsAffected() (int64, error)
}) bool {
	if result == nil {
		return false
	}
	affected, err := result.RowsAffected()
	return err == nil && affected > 0
}
