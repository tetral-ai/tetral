package tetralsandbox

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

const SandboxOutputCaptureCleanupBatchSize = 100

type SandboxOutputCaptureCleanupJob struct {
	JobID             string
	LeaseToken        string
	WorkspaceID       string
	SessionID         string
	FinishIdleWriteID string
	CaptureGeneration int64
	CleanupGeneration int64
}

type SandboxOutputCaptureCleanupWork struct {
	SandboxOutputCaptureCleanupJob
	BlobPointers []string
}

type SandboxOutputCaptureCleanupStore interface {
	SweepExpiredCaptures(context.Context, string, time.Time, int) (int, error)
	LoadCaptureCleanup(context.Context, SandboxOutputCaptureCleanupJob) (SandboxOutputCaptureCleanupWork, bool, error)
	CompleteCaptureCleanup(context.Context, SandboxOutputCaptureCleanupWork, time.Time) error
	FinalizeCaptureCleanupExhaustion(context.Context, *queuev1.QueueJob, time.Time) error
}

type SandboxOutputCaptureCleanupRunner struct {
	Queue     SandboxQueueClient
	Store     SandboxOutputCaptureCleanupStore
	BlobStore blob.BlobStore
	Config    SandboxOutputCaptureRunnerConfig
}

func (r *SandboxOutputCaptureCleanupRunner) RunOnceWithActivity(ctx context.Context) (bool, error) {
	if r == nil || r.Queue == nil || r.Store == nil || r.BlobStore == nil {
		return false, errors.New("sandbox output capture cleanup dependencies are required")
	}
	cfg := r.Config
	if cfg.LeaseOwner == "" {
		cfg.LeaseOwner = ServiceName
	}
	if cfg.MaxJobs <= 0 {
		cfg.MaxJobs = 1
	}
	if cfg.WorkspaceID == "" || cfg.LeaseDuration <= cfg.HeartbeatInterval || cfg.HeartbeatInterval <= 0 {
		return false, errors.New("sandbox output capture cleanup runner configuration is invalid")
	}
	leaseSentAt := time.Now()
	lease, err := r.Queue.Lease(ctx, &queuev1.LeaseRequest{
		WorkspaceId: cfg.WorkspaceID, Kinds: []string{queue.KindSandboxOutputCaptureCleanup},
		LeaseOwner: cfg.LeaseOwner, MaxJobs: int32(cfg.MaxJobs), LeaseDurationMs: cfg.LeaseDuration.Milliseconds(),
	})
	if err != nil {
		return false, err
	}
	for _, job := range lease.GetJobs() {
		if err := r.processJob(ctx, job, cfg, leaseSentAt.Add(wireRoundedQueueLeaseDuration(cfg.LeaseDuration))); err != nil {
			return len(lease.GetJobs()) > 0, err
		}
	}
	return len(lease.GetJobs()) > 0, nil
}

func (r *SandboxOutputCaptureCleanupRunner) processJob(ctx context.Context, queueJob *queuev1.QueueJob, cfg SandboxOutputCaptureRunnerConfig, localExpiry time.Time) (resultErr error) {
	workCtx, finishLease, err := startQueueLeaseGuard(ctx, r.Queue, queueJob, localExpiry, cfg.HeartbeatInterval, cfg.LeaseDuration)
	if err != nil {
		return err
	}
	defer func() {
		if leaseErr := finishLease(); resultErr == nil && leaseErr != nil {
			resultErr = leaseErr
		}
	}()
	ctx = workCtx
	if queueJob.GetMaxAttempts() <= 0 || queueJob.GetAttemptCount() > queueJob.GetMaxAttempts() {
		if err := r.Store.FinalizeCaptureCleanupExhaustion(ctx, queueJob, storage.Now()); err != nil {
			return err
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		return deadLetterBackgroundJob(ctx, r.Queue, queueJob, "sandbox_output_capture_cleanup_exhausted")
	}
	job, err := DecodeSandboxOutputCaptureCleanupJob(queueJob)
	if err != nil {
		if err := r.Store.FinalizeCaptureCleanupExhaustion(ctx, queueJob, storage.Now()); err != nil {
			return err
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		return deadLetterBackgroundJob(ctx, r.Queue, queueJob, "invalid_sandbox_output_capture_cleanup_payload")
	}
	err = r.cleanup(ctx, job)
	if err != nil {
		if errors.Is(err, errQueueLeaseLost) {
			return err
		}
		if queueJob.GetAttemptCount() >= queueJob.GetMaxAttempts() {
			if err := r.Store.FinalizeCaptureCleanupExhaustion(ctx, queueJob, storage.Now()); err != nil {
				return err
			}
			if err := stopQueueLeaseGuard(ctx); err != nil {
				return err
			}
			return deadLetterBackgroundJob(ctx, r.Queue, queueJob, "sandbox_output_capture_cleanup_exhausted")
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.Retry(ctx, &queuev1.RetryRequest{
			WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
			ErrorKind: "sandbox_output_capture_cleanup_retryable", ErrorMessage: "sandbox output capture cleanup will be retried",
		}))
	}
	if err := stopQueueLeaseGuard(ctx); err != nil {
		return err
	}
	return transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken}))
}

func (r *SandboxOutputCaptureCleanupRunner) cleanup(ctx context.Context, job SandboxOutputCaptureCleanupJob) error {
	work, current, err := r.Store.LoadCaptureCleanup(ctx, job)
	if err != nil || !current {
		return err
	}
	for _, pointer := range work.BlobPointers {
		if err := r.BlobStore.Delete(ctx, pointer); err != nil {
			var notFound *blob.NotFoundError
			if !errors.As(err, &notFound) {
				return err
			}
		}
	}
	return r.Store.CompleteCaptureCleanup(ctx, work, storage.Now())
}

func DecodeSandboxOutputCaptureCleanupJob(job *queuev1.QueueJob) (SandboxOutputCaptureCleanupJob, error) {
	identity, err := decodeSandboxOutputCaptureCleanupTransportIdentity(job)
	if err != nil || job.GetLeaseToken() == "" {
		return SandboxOutputCaptureCleanupJob{}, errors.New("sandbox output capture cleanup transport identity is incomplete")
	}
	var payload struct {
		WorkspaceID       string `json:"workspace_id"`
		SessionID         string `json:"session_id"`
		FinishIdleWriteID string `json:"finish_idle_write_id"`
		CaptureGeneration int64  `json:"capture_generation"`
		CleanupGeneration int64  `json:"cleanup_generation"`
	}
	decoder := json.NewDecoder(strings.NewReader(job.GetPayloadJson()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return SandboxOutputCaptureCleanupJob{}, err
	}
	if payload.WorkspaceID != identity.WorkspaceID || payload.SessionID != identity.SessionID || payload.FinishIdleWriteID != identity.FinishIdleWriteID ||
		payload.CaptureGeneration != identity.CaptureGeneration || payload.CleanupGeneration != identity.CleanupGeneration {
		return SandboxOutputCaptureCleanupJob{}, errors.New("sandbox output capture cleanup identity is invalid")
	}
	identity.LeaseToken = job.GetLeaseToken()
	return identity, nil
}

func decodeSandboxOutputCaptureTransportIdentity(job *queuev1.QueueJob) (SandboxOutputCaptureJob, error) {
	if job == nil || job.GetKind() != queue.KindSandboxOutputCapture || job.GetId() == "" || job.GetWorkspaceId() == "" {
		return SandboxOutputCaptureJob{}, errors.New("sandbox output capture transport identity is incomplete")
	}
	prefix := queue.KindSandboxOutputCapture + ":" + job.GetWorkspaceId() + ":"
	parts := strings.Split(strings.TrimPrefix(job.GetDedupeKey(), prefix), ":")
	if !strings.HasPrefix(job.GetDedupeKey(), prefix) || len(parts) != 3 {
		return SandboxOutputCaptureJob{}, errors.New("sandbox output capture transport identity is invalid")
	}
	generation, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil || generation <= 0 || job.GetPartitionKey() != queue.FormatSandboxCapturePartitionKey(queueWorkspaceID(job.GetWorkspaceId()), parts[0], parts[1]) {
		return SandboxOutputCaptureJob{}, errors.New("sandbox output capture transport identity is invalid")
	}
	return SandboxOutputCaptureJob{JobID: job.GetId(), LeaseToken: job.GetLeaseToken(), WorkspaceID: job.GetWorkspaceId(), SessionID: parts[0], FinishIdleWriteID: parts[1], CaptureGeneration: generation}, nil
}

func decodeSandboxOutputCaptureCleanupTransportIdentity(job *queuev1.QueueJob) (SandboxOutputCaptureCleanupJob, error) {
	if job == nil || job.GetKind() != queue.KindSandboxOutputCaptureCleanup || job.GetId() == "" || job.GetWorkspaceId() == "" {
		return SandboxOutputCaptureCleanupJob{}, errors.New("sandbox output capture cleanup transport identity is incomplete")
	}
	prefix := queue.KindSandboxOutputCaptureCleanup + ":" + job.GetWorkspaceId() + ":"
	parts := strings.Split(strings.TrimPrefix(job.GetDedupeKey(), prefix), ":")
	if !strings.HasPrefix(job.GetDedupeKey(), prefix) || len(parts) != 4 {
		return SandboxOutputCaptureCleanupJob{}, errors.New("sandbox output capture cleanup transport identity is invalid")
	}
	captureGeneration, captureErr := strconv.ParseInt(parts[2], 10, 64)
	cleanupGeneration, cleanupErr := strconv.ParseInt(parts[3], 10, 64)
	if captureErr != nil || cleanupErr != nil || captureGeneration <= 0 || cleanupGeneration <= 0 ||
		job.GetPartitionKey() != queue.FormatSandboxCapturePartitionKey(queueWorkspaceID(job.GetWorkspaceId()), parts[0], parts[1]) {
		return SandboxOutputCaptureCleanupJob{}, errors.New("sandbox output capture cleanup transport identity is invalid")
	}
	return SandboxOutputCaptureCleanupJob{JobID: job.GetId(), LeaseToken: job.GetLeaseToken(), WorkspaceID: job.GetWorkspaceId(), SessionID: parts[0], FinishIdleWriteID: parts[1], CaptureGeneration: captureGeneration, CleanupGeneration: cleanupGeneration}, nil
}

func (s *PostgreSQLSandboxOutputCaptureStore) SweepExpiredCaptures(ctx context.Context, workspaceID string, now time.Time, limit int) (int, error) {
	if s == nil || s.client == nil || workspaceID == "" {
		return 0, errors.New("sandbox output capture database is required")
	}
	if limit <= 0 || limit > SandboxOutputCaptureCleanupBatchSize {
		limit = SandboxOutputCaptureCleanupBatchSize
	}
	count := 0
	err := s.client.WithWorkspaceTx(ctx, workspaceID, "sandbox.output_capture.sweep_expired", func(tx *dbconnect.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT session_id, finish_idle_write_id, capture_generation, cleanup_generation
			   FROM sandbox_output_capture_operations
			  WHERE workspace_id=$1 AND state IN ('staged','skipped_unavailable','failed') AND retain_until <= $2
			  ORDER BY retain_until, session_id, finish_idle_write_id, capture_generation
			  LIMIT $3 FOR UPDATE SKIP LOCKED`, workspaceID, now.UTC(), limit)
		if err != nil {
			return err
		}
		type candidate struct {
			sessionID, writeID string
			captureGeneration  int64
			cleanupGeneration  int64
		}
		var candidates []candidate
		for rows.Next() {
			var candidate candidate
			if err := rows.Scan(&candidate.sessionID, &candidate.writeID, &candidate.captureGeneration, &candidate.cleanupGeneration); err != nil {
				_ = rows.Close()
				return err
			}
			candidates = append(candidates, candidate)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if err := rows.Err(); err != nil {
			return err
		}
		for _, candidate := range candidates {
			nextGeneration := candidate.cleanupGeneration + 1
			result, err := tx.Exec(ctx,
				`UPDATE sandbox_output_capture_operations SET state='cleanup_pending', cleanup_generation=$5, updated_at=$6
				  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4
				    AND state IN ('staged','skipped_unavailable','failed') AND retain_until <= $6`,
				workspaceID, candidate.sessionID, candidate.writeID, candidate.captureGeneration, nextGeneration, now.UTC())
			if err != nil {
				return err
			}
			if !transitionRowsAffected(result) {
				continue
			}
			if err := queue.EnqueueSandboxOutputCaptureCleanupTx(ctx, tx, workspace.ID(workspaceID), candidate.sessionID, candidate.writeID, candidate.captureGeneration, nextGeneration, now); err != nil {
				return err
			}
			count++
		}
		return nil
	})
	return count, err
}

func (s *PostgreSQLSandboxOutputCaptureStore) LoadCaptureCleanup(ctx context.Context, job SandboxOutputCaptureCleanupJob) (SandboxOutputCaptureCleanupWork, bool, error) {
	work := SandboxOutputCaptureCleanupWork{SandboxOutputCaptureCleanupJob: job}
	current := false
	err := s.client.WithWorkspaceTx(ctx, job.WorkspaceID, "sandbox.output_capture.load_cleanup", func(tx *dbconnect.Tx) (txErr error) {
		defer finishSandboxQueueAuthorityTx(ctx, tx, &txErr)
		if err := lockOutputCaptureSessionTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
			return err
		}
		if _, _, err := lockSandboxBinding(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
			return err
		}
		var state string
		var generation int64
		if err := tx.QueryRow(ctx,
			`SELECT state, cleanup_generation FROM sandbox_output_capture_operations
			  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4
			  FOR UPDATE`,
			job.WorkspaceID, job.SessionID, job.FinishIdleWriteID, job.CaptureGeneration).Scan(&state, &generation); dbconnect.IsNoRows(err) {
			return nil
		} else if err != nil {
			return err
		}
		if state != "cleanup_pending" || generation != job.CleanupGeneration {
			return nil
		}
		rows, err := tx.Query(ctx,
			`SELECT blob_pointer FROM sandbox_output_capture_blobs
			  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4
			  ORDER BY source_path FOR UPDATE`, job.WorkspaceID, job.SessionID, job.FinishIdleWriteID, job.CaptureGeneration)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var pointer string
			if err := rows.Scan(&pointer); err != nil {
				return err
			}
			work.BlobPointers = append(work.BlobPointers, pointer)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		current = true
		return nil
	})
	return work, current, err
}

func (s *PostgreSQLSandboxOutputCaptureStore) CompleteCaptureCleanup(ctx context.Context, work SandboxOutputCaptureCleanupWork, now time.Time) error {
	return s.client.WithWorkspaceTx(ctx, work.WorkspaceID, "sandbox.output_capture.complete_cleanup", func(tx *dbconnect.Tx) (txErr error) {
		defer finishSandboxQueueAuthorityTx(ctx, tx, &txErr)
		if err := lockOutputCaptureSessionTx(ctx, tx, work.WorkspaceID, work.SessionID); err != nil {
			return err
		}
		if _, _, err := lockSandboxBinding(ctx, tx, work.WorkspaceID, work.SessionID); err != nil {
			return err
		}
		var state string
		var cleanupGeneration int64
		if err := tx.QueryRow(ctx,
			`SELECT state, cleanup_generation FROM sandbox_output_capture_operations
			  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4 FOR UPDATE`,
			work.WorkspaceID, work.SessionID, work.FinishIdleWriteID, work.CaptureGeneration).Scan(&state, &cleanupGeneration); err != nil {
			return err
		}
		if state == "cleaned" {
			return nil
		}
		if state != "cleanup_pending" || cleanupGeneration != work.CleanupGeneration {
			return errors.New("sandbox output capture cleanup lost its generation fence")
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM sandbox_output_capture_blobs
			  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4`,
			work.WorkspaceID, work.SessionID, work.FinishIdleWriteID, work.CaptureGeneration); err != nil {
			return err
		}
		result, err := tx.Exec(ctx,
			`UPDATE sandbox_output_capture_operations SET state='cleaned', cleaned_at=$6, updated_at=$6
			  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4
			    AND state='cleanup_pending' AND cleanup_generation=$5`,
			work.WorkspaceID, work.SessionID, work.FinishIdleWriteID, work.CaptureGeneration, work.CleanupGeneration, now.UTC())
		if err != nil {
			return err
		}
		if !transitionRowsAffected(result) {
			return errors.New("sandbox output capture cleanup lost its state fence")
		}
		return nil
	})
}

func (s *PostgreSQLSandboxOutputCaptureStore) FinalizeCaptureCleanupExhaustion(ctx context.Context, job *queuev1.QueueJob, now time.Time) error {
	identity, err := decodeSandboxOutputCaptureCleanupTransportIdentity(job)
	if err != nil {
		return err
	}
	return s.client.WithWorkspaceTx(ctx, identity.WorkspaceID, "sandbox.output_capture.advance_cleanup", func(tx *dbconnect.Tx) (txErr error) {
		defer finishSandboxQueueAuthorityTx(ctx, tx, &txErr)
		if err := lockOutputCaptureSessionTx(ctx, tx, identity.WorkspaceID, identity.SessionID); err != nil {
			return err
		}
		if _, _, err := lockSandboxBinding(ctx, tx, identity.WorkspaceID, identity.SessionID); err != nil {
			return err
		}
		return advanceOutputCaptureCleanupTx(ctx, tx, identity, now)
	})
}

func advanceOutputCaptureCleanupTx(ctx context.Context, tx *dbconnect.Tx, identity SandboxOutputCaptureCleanupJob, now time.Time) error {
	var state string
	var cleanupGeneration int64
	err := tx.QueryRow(ctx,
		`SELECT state, cleanup_generation FROM sandbox_output_capture_operations
		  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4 FOR UPDATE`,
		identity.WorkspaceID, identity.SessionID, identity.FinishIdleWriteID, identity.CaptureGeneration).Scan(&state, &cleanupGeneration)
	if dbconnect.IsNoRows(err) || (err == nil && (state != "cleanup_pending" || cleanupGeneration != identity.CleanupGeneration)) {
		return nil
	}
	if err != nil {
		return err
	}
	nextGeneration := cleanupGeneration + 1
	if _, err := tx.Exec(ctx,
		`UPDATE sandbox_output_capture_operations SET cleanup_generation=$5, updated_at=$6
		  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4`,
		identity.WorkspaceID, identity.SessionID, identity.FinishIdleWriteID, identity.CaptureGeneration, nextGeneration, now.UTC()); err != nil {
		return err
	}
	return queue.EnqueueSandboxOutputCaptureCleanupTx(ctx, tx, workspace.ID(identity.WorkspaceID), identity.SessionID, identity.FinishIdleWriteID, identity.CaptureGeneration, nextGeneration, now)
}

func finalizeOutputCaptureExhaustionTx(ctx context.Context, tx *dbconnect.Tx, identity SandboxOutputCaptureJob, now time.Time) error {
	digest := outputCaptureOutcomeDigest("failed", "[]", "[]", "[]", "sandbox_output_capture_attempts_exhausted", "sandbox output capture attempt budget exhausted")
	_, err := tx.Exec(ctx,
		`UPDATE sandbox_output_capture_operations SET state='failed', failure_kind='sandbox_output_capture_attempts_exhausted',
		    failure_detail='sandbox output capture attempt budget exhausted', outcome_state='failed', outcome_digest=$5, updated_at=$6
		  WHERE workspace_id=$1 AND session_id=$2 AND finish_idle_write_id=$3 AND capture_generation=$4 AND state IN ('pending','running')`,
		identity.WorkspaceID, identity.SessionID, identity.FinishIdleWriteID, identity.CaptureGeneration, digest, now.UTC())
	return err
}
