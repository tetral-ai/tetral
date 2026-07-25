package tetralsandbox

import (
	"context"
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
	err := s.client.WithWorkspaceTx(ctx, job.WorkspaceID, "sandbox.environment_build.claim", func(tx *dbconnect.Tx) error {
		var (
			status            string
			packagesJSON      string
			artifactInputHash string
		)
		err := tx.QueryRow(ctx,
			`SELECT status, packages_json, artifact_input_hash
			   FROM environment_artifacts
			  WHERE workspace_id = $1
			    AND environment_id = $2
			    AND generation = $3
			  FOR UPDATE`,
			job.WorkspaceID, job.EnvironmentID, job.Generation,
		).Scan(&status, &packagesJSON, &artifactInputHash)
		if dbconnect.IsNoRows(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if status == "ready" || status == "failed" {
			return nil
		}
		packages, err := decodePackageSetupJSON(packagesJSON)
		if err != nil {
			return err
		}
		result, err := tx.Exec(ctx,
			`UPDATE environment_artifacts
			    SET status = 'building',
			        provider_artifact_ref = NULL,
			        failure_stage = NULL,
			        last_error_kind = NULL,
			        failure_reason = NULL,
			        retryable = NULL,
			        updated_at = $4
			  WHERE workspace_id = $1
			    AND environment_id = $2
			    AND generation = $3
			    AND status IN ('pending', 'building')`,
			job.WorkspaceID, job.EnvironmentID, job.Generation, now.UTC(),
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
			ArtifactInputHash:  artifactInputHash,
			NormalizedPackages: packages,
		}
		claimed = true
		return nil
	})
	return input, claimed, err
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
	return s.client.WithWorkspaceTx(ctx, job.WorkspaceID, "sandbox.environment_build.ready", func(tx *dbconnect.Tx) error {
		var status string
		var artifactInputHash string
		err := tx.QueryRow(ctx,
			`SELECT status, artifact_input_hash
			   FROM environment_artifacts
			  WHERE workspace_id = $1
			    AND environment_id = $2
			    AND generation = $3
			  FOR UPDATE`,
			job.WorkspaceID, job.EnvironmentID, job.Generation,
		).Scan(&status, &artifactInputHash)
		if dbconnect.IsNoRows(err) {
			return nil
		}
		if err != nil {
			return err
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

func (s *EnvironmentArtifactStore) MarkEnvironmentBuildRetryableFailure(ctx context.Context, job EnvironmentBuildJob, failure EnvironmentArtifactFailure, now time.Time) error {
	if s == nil || s.client == nil {
		return errors.New("environment artifact store is required")
	}
	if now.IsZero() {
		now = storage.Now()
	}
	return s.client.WithWorkspaceTx(ctx, job.WorkspaceID, "sandbox.environment_build.retryable_failure", func(tx *dbconnect.Tx) error {
		_, err := tx.Exec(ctx,
			`UPDATE environment_artifacts
			    SET status = 'pending',
			        failure_stage = $4,
			        last_error_kind = $5,
			        failure_reason = $6,
			        retryable = TRUE,
			        updated_at = $7
			  WHERE workspace_id = $1
			    AND environment_id = $2
			    AND generation = $3
			    AND status = 'building'`,
			job.WorkspaceID, job.EnvironmentID, job.Generation,
			nullIfEmpty(failure.Stage), nullIfEmpty(failure.LastErrorKind), nullIfEmpty(failure.Reason), now.UTC(),
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
	return s.client.WithWorkspaceTx(ctx, job.WorkspaceID, "sandbox.environment_build.terminal_failure", func(tx *dbconnect.Tx) error {
		timestamp := now.UTC()
		var artifactInputHash string
		if err := tx.QueryRow(ctx,
			`SELECT artifact_input_hash
			   FROM environment_artifacts
			  WHERE workspace_id = $1 AND environment_id = $2 AND generation = $3
			  FOR UPDATE`,
			job.WorkspaceID, job.EnvironmentID, job.Generation,
		).Scan(&artifactInputHash); dbconnect.IsNoRows(err) {
			return nil
		} else if err != nil {
			return err
		}
		rows, err := tx.Query(ctx,
			`UPDATE environment_artifacts
			    SET status = 'failed',
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
		rows, err = tx.Query(ctx,
			`UPDATE session_preparations
			    SET status = 'failed',
			        failure_stage = $4,
			        last_error_kind = $5,
			        failure_reason = $6,
			        retryable = FALSE,
			        failed_at = $7,
			        updated_at = $7
			  WHERE workspace_id = $1
			    AND environment_id = $2
			    AND environment_generation = ANY($3)
			    AND status = 'waiting_environment'
			    AND superseded_at IS NULL
			RETURNING environment_generation`,
			job.WorkspaceID, job.EnvironmentID, generations,
			nullIfEmpty(failure.Stage), nullIfEmpty(failure.LastErrorKind), nullIfEmpty(failure.Reason), timestamp,
		)
		if err != nil {
			return err
		}
		failedGenerations := map[int64]struct{}{}
		for rows.Next() {
			var generation int64
			if err := rows.Scan(&generation); err != nil {
				_ = rows.Close()
				return err
			}
			failedGenerations[generation] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for generation := range failedGenerations {
			if err := enqueueEnvironmentFailedFanout(ctx, tx, EnvironmentBuildJob{
				WorkspaceID: job.WorkspaceID, EnvironmentID: job.EnvironmentID, Generation: generation,
			}, now.UTC()); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *EnvironmentArtifactStore) FanoutReadyEnvironment(ctx context.Context, job EnvironmentReadyFanoutJob, now time.Time) (int, error) {
	if s == nil || s.client == nil {
		return 0, errors.New("environment artifact store is required")
	}
	if now.IsZero() {
		now = storage.Now()
	}
	var advanced int
	err := s.client.WithWorkspaceTx(ctx, job.WorkspaceID, "sandbox.environment_ready_fanout", func(tx *dbconnect.Tx) error {
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
		rows, err := tx.Query(ctx,
			`UPDATE session_preparations
			    SET status = 'pending',
			        updated_at = $4
			  WHERE workspace_id = $1
			    AND environment_id = $2
			    AND environment_generation = $3
			    AND status = 'waiting_environment'
			    AND superseded_at IS NULL
			RETURNING session_id, preparation_attempt_id`,
			job.WorkspaceID, job.EnvironmentID, job.Generation, now.UTC(),
		)
		if err != nil {
			return err
		}
		type preparationRef struct {
			sessionID            string
			preparationAttemptID string
		}
		var preparations []preparationRef
		for rows.Next() {
			var ref preparationRef
			if err := rows.Scan(&ref.sessionID, &ref.preparationAttemptID); err != nil {
				_ = rows.Close()
				return err
			}
			preparations = append(preparations, ref)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, ref := range preparations {
			if err := enqueueSessionPrepareForFanout(ctx, tx, job.WorkspaceID, ref.sessionID, ref.preparationAttemptID, now.UTC()); err != nil {
				return err
			}
			advanced++
		}
		return nil
	})
	return advanced, err
}

// FanoutFailedEnvironment settles the gated inputs of every session whose
// preparation failed for this environment generation. It lists the failed
// preparations in one read transaction, then settles each session in its own
// transaction:
//
//	step    lock                                  effect
//	fence   sessions row FOR UPDATE               skip if lifecycle_state deleted
//	verify  session_preparations row FOR UPDATE   skip unless still failed for
//	                                              this environment + generation
//	settle  (no further lock)                     load pending runtime-input
//	                                              segments and enqueue them
//
// The loaded segments carry each event's own birth preparation_attempt_id:
// LoadPendingRuntimeInputSegmentsThroughPreparationAttempt selects
// still-unprocessed events whose recorded birth is at-or-before the failed
// attempt, and EnqueueRuntimeInputSegments copies those births into the
// runtime_input jobs and never reassigns them. Retry is idempotent: the load
// step excludes events already referenced by a pending or leased runtime_input
// job. UPDATE-WITH: internal/sandbox
// (LoadPendingRuntimeInputSegmentsThroughPreparationAttempt,
// EnqueueRuntimeInputSegments), environment_failed_fanout_runner.go.
func (s *EnvironmentArtifactStore) FanoutFailedEnvironment(ctx context.Context, job EnvironmentFailedFanoutJob, now time.Time) (int, error) {
	if s == nil || s.client == nil {
		return 0, errors.New("environment artifact store is required")
	}
	if now.IsZero() {
		now = storage.Now()
	}
	type preparationRef struct {
		sessionID            string
		preparationAttemptID string
	}
	var preparations []preparationRef
	if err := s.client.WithWorkspaceTx(ctx, job.WorkspaceID, "sandbox.environment_failed_fanout.list", func(tx *dbconnect.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT session_id, preparation_attempt_id
			   FROM session_preparations
			  WHERE workspace_id = $1
			    AND environment_id = $2
			    AND environment_generation = $3
			    AND status = 'failed'
			  ORDER BY session_id, preparation_attempt_id`,
			job.WorkspaceID, job.EnvironmentID, job.Generation,
		)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var ref preparationRef
			if err := rows.Scan(&ref.sessionID, &ref.preparationAttemptID); err != nil {
				return err
			}
			preparations = append(preparations, ref)
		}
		return rows.Err()
	}); err != nil {
		return 0, err
	}
	fannedOut := 0
	for _, ref := range preparations {
		err := s.client.WithWorkspaceTx(ctx, job.WorkspaceID, "sandbox.environment_failed_fanout.session", func(tx *dbconnect.Tx) error {
			var lifecycleState string
			if err := tx.QueryRow(ctx,
				`SELECT lifecycle_state
				   FROM sessions
				  WHERE workspace_id = $1
				    AND id = $2
				  FOR UPDATE`,
				job.WorkspaceID, ref.sessionID,
			).Scan(&lifecycleState); dbconnect.IsNoRows(err) {
				return nil
			} else if err != nil {
				return err
			}
			if lifecycleState == "deleted" {
				return nil
			}
			var failedStatus string
			var failedEnvironmentID string
			var failedEnvironmentGeneration int64
			err := tx.QueryRow(ctx,
				`SELECT status, environment_id, environment_generation
				   FROM session_preparations
				  WHERE workspace_id = $1
				    AND session_id = $2
				    AND preparation_attempt_id = $3
				  FOR UPDATE`,
				job.WorkspaceID, ref.sessionID, ref.preparationAttemptID,
			).Scan(&failedStatus, &failedEnvironmentID, &failedEnvironmentGeneration)
			if dbconnect.IsNoRows(err) {
				return nil
			}
			if err != nil {
				return err
			}
			if failedStatus != "failed" ||
				failedEnvironmentID != job.EnvironmentID ||
				failedEnvironmentGeneration != job.Generation {
				return nil
			}
			segments, err := sandbox.LoadPendingRuntimeInputSegmentsThroughPreparationAttempt(
				ctx,
				tx,
				workspace.ID(job.WorkspaceID),
				ref.sessionID,
				ref.preparationAttemptID,
			)
			if err != nil {
				return err
			}
			if err := sandbox.EnqueueRuntimeInputSegments(ctx, tx, workspace.ID(job.WorkspaceID), ref.sessionID, segments, now.UTC()); err != nil {
				return err
			}
			fannedOut++
			return nil
		})
		if err != nil {
			return fannedOut, err
		}
	}
	return fannedOut, nil
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

func enqueueEnvironmentFailedFanout(ctx context.Context, tx *dbconnect.Tx, job EnvironmentBuildJob, now time.Time) error {
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
		Kind:           queue.KindEnvironmentFailedFanout,
		PartitionKey:   queue.FormatEnvironmentPartitionKey(ws, job.EnvironmentID),
		DedupeKey:      queue.FormatEnvironmentFailedFanoutDedupeKey(ws, job.EnvironmentID, strconv.FormatInt(job.Generation, 10)),
		PayloadVersion: 1,
		PayloadJSON:    payload,
		Now:            now,
	})
	return err
}

func enqueueSessionPrepareForFanout(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, preparationAttemptID string, now time.Time) error {
	payload, err := json.Marshal(map[string]string{
		"workspace_id":           workspaceID,
		"session_id":             sessionID,
		"preparation_attempt_id": preparationAttemptID,
	})
	if err != nil {
		return err
	}
	ws := workspace.ID(workspaceID)
	_, err = queue.EnqueueTx(ctx, tx, queue.EnqueueRequest{
		WorkspaceID:    ws,
		Kind:           queue.KindSessionPrepare,
		PartitionKey:   queue.FormatSessionPartitionKey(ws, sessionID),
		DedupeKey:      queue.FormatSessionPrepareDedupeKey(ws, sessionID, preparationAttemptID),
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
