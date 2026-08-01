package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type PostgreSQLQueueStore struct {
	client      *dbconnect.Client
	retryPolicy RetryPolicy
}

func NewPostgreSQLStore(client *dbconnect.Client) *PostgreSQLQueueStore {
	return NewPostgreSQLStoreWithRetryPolicy(client, RetryPolicy{})
}

type RetryPolicy struct {
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	MaxAttempts int
	RandomInt64 func(int64) int64
}

func NewPostgreSQLStoreWithRetryPolicy(client *dbconnect.Client, policy RetryPolicy) *PostgreSQLQueueStore {
	return &PostgreSQLQueueStore{client: client, retryPolicy: normalizeRetryPolicy(policy)}
}

func normalizeRetryPolicy(policy RetryPolicy) RetryPolicy {
	if policy.BaseDelay <= 0 {
		policy.BaseDelay = time.Second
	}
	if policy.MaxDelay <= 0 {
		policy.MaxDelay = time.Minute
	}
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = DefaultMaxAttempts
	}
	if policy.RandomInt64 == nil {
		policy.RandomInt64 = rand.Int64N
	}
	return policy
}

func (s *PostgreSQLQueueStore) VerifyReady(ctx context.Context) error {
	if s == nil || s.client == nil {
		return &ValidationError{Message: "queue store is required"}
	}
	rows, err := s.client.Query(ctx, "queue.verify_ready", "SELECT 1 FROM queue_jobs LIMIT 0")
	if err != nil {
		return err
	}
	return rows.Close()
}

func (s *PostgreSQLQueueStore) Metrics(ctx context.Context, now time.Time) ([]MetricsSnapshot, error) {
	if s == nil || s.client == nil {
		return nil, &ValidationError{Message: "queue store is required"}
	}
	if now.IsZero() {
		now = storage.Now()
	}
	now = now.UTC()
	var snapshots []MetricsSnapshot
	err := s.client.WithTx(ctx, "queue.metrics", nil, func(tx *dbconnect.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT set_config('tetral.queue_maintenance', 'true', true)"); err != nil {
			return err
		}
		rows, err := tx.Query(ctx,
			`SELECT kind,
			        COUNT(*) FILTER (WHERE status = 'pending') AS pending_jobs,
			        COUNT(*) FILTER (WHERE status = 'leased') AS leased_jobs,
			        COUNT(*) FILTER (WHERE status = 'pending' AND attempt_count > 0) AS retry_pending_jobs,
			        COUNT(*) FILTER (WHERE status = 'dead_lettered') AS dead_lettered_jobs,
			        COALESCE(MAX(
			            CASE
			                WHEN status = 'pending' AND available_at <= $1
			                    THEN EXTRACT(EPOCH FROM ($1 - available_at))
			                ELSE 0
			            END
			        ), 0) AS ready_lag_seconds
			   FROM queue_jobs
			  WHERE status IN ('pending', 'leased', 'dead_lettered')
			  GROUP BY kind
			  ORDER BY kind ASC`,
			now,
		)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var snapshot MetricsSnapshot
			if err := rows.Scan(
				&snapshot.Kind,
				&snapshot.PendingJobs,
				&snapshot.LeasedJobs,
				&snapshot.RetryPendingJobs,
				&snapshot.DeadLetteredJobs,
				&snapshot.ReadyLagSeconds,
			); err != nil {
				return err
			}
			snapshots = append(snapshots, snapshot)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return snapshots, nil
}

func (s *PostgreSQLQueueStore) Enqueue(ctx context.Context, request EnqueueRequest) (*Job, error) {
	request, err := NormalizeEnqueueRequest(request)
	if err != nil {
		return nil, err
	}
	if s == nil || s.client == nil {
		return nil, &ValidationError{Message: "queue store is required"}
	}
	var job *Job
	err = s.client.WithWorkspaceTx(ctx, string(request.WorkspaceID), "queue.enqueue", func(tx *dbconnect.Tx) error {
		inserted, err := EnqueueTx(ctx, tx, request)
		if err != nil {
			return err
		}
		job = inserted
		return nil
	})
	if err != nil {
		return nil, err
	}
	return job, nil
}

type enqueueTransaction interface {
	QueryRowScanner(ctx context.Context, query string, args ...any) interface{ Scan(dest ...any) error }
}

type queueMutationTransaction interface {
	enqueueTransaction
	Exec(ctx context.Context, query string, args ...any) (sql.Result, error)
}

const sandboxJobKindSQLList = "'sandbox_tool_execute', 'sandbox_activate', 'sandbox_materialize', 'sandbox_release', 'sandbox_tool_cancel', 'sandbox_output_capture', 'sandbox_output_capture_cleanup', 'sandbox_memory_projection', 'sandbox_background_command', 'sandbox_background_reconcile'"

func EnqueueTx(ctx context.Context, tx enqueueTransaction, request EnqueueRequest) (*Job, error) {
	jobs, err := EnqueueBatchTx(ctx, tx, []EnqueueRequest{request})
	if err != nil {
		return nil, err
	}
	return jobs[0], nil
}

func EnqueueSandboxOutputCaptureCleanupTx(
	ctx context.Context,
	tx enqueueTransaction,
	workspaceID workspace.ID,
	sessionID string,
	finishIdleWriteID string,
	captureGeneration int64,
	cleanupGeneration int64,
	now time.Time,
) error {
	payload, err := json.Marshal(struct {
		WorkspaceID       string `json:"workspace_id"`
		SessionID         string `json:"session_id"`
		FinishIdleWriteID string `json:"finish_idle_write_id"`
		CaptureGeneration int64  `json:"capture_generation"`
		CleanupGeneration int64  `json:"cleanup_generation"`
	}{
		WorkspaceID: string(workspaceID), SessionID: sessionID, FinishIdleWriteID: finishIdleWriteID,
		CaptureGeneration: captureGeneration, CleanupGeneration: cleanupGeneration,
	})
	if err != nil {
		return err
	}
	_, err = EnqueueTx(ctx, tx, EnqueueRequest{
		ID: NewJobID(), WorkspaceID: workspaceID, Kind: KindSandboxOutputCaptureCleanup,
		PartitionKey:   FormatSandboxCapturePartitionKey(workspaceID, sessionID, finishIdleWriteID),
		DedupeKey:      FormatSandboxOutputCaptureCleanupDedupeKey(workspaceID, sessionID, finishIdleWriteID, captureGeneration, cleanupGeneration),
		PayloadVersion: 1, PayloadJSON: payload, MaxAttempts: SandboxOutputCaptureCleanupMaxAttempts, Now: now,
	})
	return err
}

// CancelTx conditionally cancels one exact pending Queue notification inside
// its owning business transaction. It never searches by a partial business
// identity and never changes leased or terminal work.
func CancelTx(ctx context.Context, tx queueMutationTransaction, request TargetedCancelRequest) (bool, error) {
	if tx == nil {
		return false, &ValidationError{Message: "queue transaction is required"}
	}
	if request.WorkspaceID == "" || request.JobID == "" || request.Kind == "" || request.PartitionKey == "" || request.DedupeKey == "" {
		return false, &ValidationError{Message: "complete targeted queue identity is required"}
	}
	if request.Now.IsZero() {
		request.Now = storage.Now()
	}
	var kind string
	var partitionKey string
	var dedupeKey sql.NullString
	var status string
	err := tx.QueryRowScanner(ctx,
		`SELECT kind, partition_key, dedupe_key, status
		   FROM queue_jobs
		  WHERE workspace_id = $1 AND id = $2
		  FOR UPDATE`,
		string(request.WorkspaceID),
		request.JobID,
	).Scan(&kind, &partitionKey, &dedupeKey, &status)
	if dbconnect.IsNoRows(err) {
		return false, &IntegrityError{Message: "targeted queue job does not exist"}
	}
	if err != nil {
		return false, err
	}
	if kind != request.Kind || partitionKey != request.PartitionKey || !dedupeKey.Valid || dedupeKey.String != request.DedupeKey {
		return false, &IntegrityError{Message: "targeted queue job identity does not match"}
	}
	if status != StatusPending {
		return false, nil
	}
	result, err := tx.Exec(ctx,
		`UPDATE queue_jobs
		    SET status = 'cancelled',
		        cancelled_at = $3,
		        updated_at = $3
		  WHERE workspace_id = $1
		    AND id = $2
		    AND status = 'pending'`,
		string(request.WorkspaceID),
		request.JobID,
		request.Now.UTC(),
	)
	if err != nil {
		return false, err
	}
	return rowsAffected(result), nil
}

// DeadLetterExhaustedTx closes one crash-reclaimed pending job only when its
// observed attempt count is still current and its explicit budget is spent.
func DeadLetterExhaustedTx(ctx context.Context, tx queueMutationTransaction, request DeadLetterExhaustedRequest) (bool, error) {
	if tx == nil {
		return false, &ValidationError{Message: "queue transaction is required"}
	}
	if request.WorkspaceID == "" || request.JobID == "" || request.ObservedAttemptCount <= 0 {
		return false, &ValidationError{Message: "workspace_id, job_id, and a positive observed attempt count are required"}
	}
	if request.Now.IsZero() {
		request.Now = storage.Now()
	}
	result, err := tx.Exec(ctx,
		`UPDATE queue_jobs
		    SET status = 'dead_lettered',
		        dead_lettered_at = $4,
		        last_error_kind = $5,
		        last_error_message = $6,
		        updated_at = $4
		  WHERE workspace_id = $1
		    AND id = $2
		    AND status = 'pending'
		    AND attempt_count = $3
		    AND max_attempts > 0
		    AND attempt_count >= max_attempts`,
		string(request.WorkspaceID),
		request.JobID,
		request.ObservedAttemptCount,
		request.Now.UTC(),
		nullableString(request.ErrorKind),
		nullableString(request.ErrorMessage),
	)
	if err != nil {
		return false, err
	}
	return rowsAffected(result), nil
}

type queuePartitionCounterKey struct {
	workspaceID  workspace.ID
	partitionKey string
}

// EnqueueBatchTx validates the complete request set and locks every partition
// counter in deterministic order before allocating jobs in caller order.
func EnqueueBatchTx(ctx context.Context, tx enqueueTransaction, requests []EnqueueRequest) ([]*Job, error) {
	normalized := make([]EnqueueRequest, len(requests))
	for index, request := range requests {
		var err error
		normalized[index], err = NormalizeEnqueueRequest(request)
		if err != nil {
			return nil, err
		}
	}
	if len(normalized) == 0 {
		return nil, nil
	}
	if tx == nil {
		return nil, &ValidationError{Message: "queue transaction is required"}
	}
	distinct := make(map[queuePartitionCounterKey]struct{}, len(normalized))
	for _, request := range normalized {
		distinct[queuePartitionCounterKey{workspaceID: request.WorkspaceID, partitionKey: request.PartitionKey}] = struct{}{}
	}
	keys := make([]queuePartitionCounterKey, 0, len(distinct))
	for key := range distinct {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool {
		if keys[left].workspaceID != keys[right].workspaceID {
			return keys[left].workspaceID < keys[right].workspaceID
		}
		return keys[left].partitionKey < keys[right].partitionKey
	})
	for _, key := range keys {
		var lockedSequence int64
		if err := tx.QueryRowScanner(ctx,
			`INSERT INTO queue_partition_counters (
				workspace_id, partition_key, last_sequence, created_at, updated_at
			) VALUES ($1, $2, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
			ON CONFLICT (workspace_id, partition_key) DO UPDATE
			SET partition_key = EXCLUDED.partition_key
			RETURNING last_sequence`,
			string(key.workspaceID),
			key.partitionKey,
		).Scan(&lockedSequence); err != nil {
			return nil, err
		}
	}

	jobs := make([]*Job, 0, len(normalized))
	for _, request := range normalized {
		if request.DedupeKey != "" {
			existing, err := existingActiveDedupeJob(ctx, tx, request.WorkspaceID, request.DedupeKey)
			if err == nil {
				jobs = append(jobs, existing)
				continue
			}
			if !dbconnect.IsNoRows(err) {
				return nil, err
			}
		}
		var partitionSequence int64
		if err := tx.QueryRowScanner(ctx,
			`UPDATE queue_partition_counters
			    SET last_sequence = last_sequence + 1,
			        updated_at = CURRENT_TIMESTAMP
			  WHERE workspace_id = $1 AND partition_key = $2
			RETURNING last_sequence`,
			string(request.WorkspaceID),
			request.PartitionKey,
		).Scan(&partitionSequence); err != nil {
			return nil, err
		}
		job, err := insertQueueJobTx(ctx, tx, request, partitionSequence)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func insertQueueJobTx(ctx context.Context, tx enqueueTransaction, request EnqueueRequest, partitionSequence int64) (*Job, error) {
	payload := append(json.RawMessage(nil), request.PayloadJSON...)
	row := tx.QueryRowScanner(ctx,
		`INSERT INTO queue_jobs (
			id, workspace_id, kind, partition_key, queue_partition_sequence, dedupe_key,
			payload_version, status, payload_json, priority, attempt_count, max_attempts,
			available_at, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8, $9, 0, $10, $11, $12, $12)
		ON CONFLICT (workspace_id, dedupe_key) WHERE status IN ('pending', 'leased') DO NOTHING
		RETURNING id, workspace_id, kind, partition_key, queue_partition_sequence, dedupe_key, payload_version,
		          status, payload_json, priority, available_at, leased_by, lease_token,
		          leased_at, leased_until, attempt_count, max_attempts, created_at, updated_at`,
		request.ID,
		string(request.WorkspaceID),
		request.Kind,
		request.PartitionKey,
		partitionSequence,
		nullableString(request.DedupeKey),
		request.PayloadVersion,
		string(payload),
		request.Priority,
		request.MaxAttempts,
		request.AvailableAt,
		request.Now,
	)
	job, err := scanJob(row)
	if err == nil {
		return job, nil
	}
	if !dbconnect.IsNoRows(err) || request.DedupeKey == "" {
		return nil, err
	}
	return existingActiveDedupeJob(ctx, tx, request.WorkspaceID, request.DedupeKey)
}

func existingActiveDedupeJob(ctx context.Context, tx enqueueTransaction, workspaceID workspace.ID, dedupeKey string) (*Job, error) {
	if tx == nil {
		return nil, &ValidationError{Message: "queue transaction is required"}
	}
	return scanJob(tx.QueryRowScanner(ctx,
		`SELECT id, workspace_id, kind, partition_key, queue_partition_sequence, dedupe_key, payload_version,
		        status, payload_json, priority, available_at, leased_by, lease_token,
		        leased_at, leased_until, attempt_count, max_attempts, created_at, updated_at
		   FROM queue_jobs
		  WHERE workspace_id = $1
		    AND dedupe_key = $2
		    AND status IN ('pending', 'leased')
		  ORDER BY queue_partition_sequence ASC
		  LIMIT 1`,
		string(workspaceID),
		dedupeKey,
	))
}

func (s *PostgreSQLQueueStore) Lease(ctx context.Context, request LeaseRequest) ([]*Job, error) {
	if err := ValidateLeaseRequest(request); err != nil {
		return nil, err
	}
	if s == nil || s.client == nil {
		return nil, &ValidationError{Message: "queue store is required"}
	}
	if request.Now.IsZero() {
		request.Now = storage.Now()
	}
	request.Now = request.Now.UTC()
	var leased []*Job
	err := s.client.WithWorkspaceTx(ctx, string(request.WorkspaceID), "queue.lease", func(tx *dbconnect.Tx) error {
		kindPredicate, args := leaseKindPredicate(request.Kinds, 4)
		query := `SELECT candidate.id, candidate.partition_key, candidate.kind, candidate.priority,
		                candidate.available_at, candidate.queue_partition_sequence,
		                COALESCE(candidate.payload_json::jsonb ->> 'input_kind', '')
			   FROM queue_jobs candidate
			  WHERE candidate.workspace_id = $1
			    AND candidate.kind IN (` + kindPredicate + `)
			    AND candidate.status = 'pending'
			    AND candidate.available_at <= $2
			    AND NOT EXISTS (
			        SELECT 1
			          FROM queue_jobs pending
			         WHERE pending.workspace_id = candidate.workspace_id
			           AND pending.partition_key = candidate.partition_key
			           AND pending.status = 'pending'
			           AND pending.id <> candidate.id
			           AND (
			                pending.available_at <= $2
			                OR (
			                    candidate.kind = 'runtime_input'
			                    AND candidate.payload_json::jsonb ->> 'input_kind' IS DISTINCT FROM 'interrupt_control'
			                    AND pending.kind = 'runtime_config_update'
			                    AND pending.queue_partition_sequence < candidate.queue_partition_sequence
			                )
			           )
			           AND (
			                (candidate.kind = 'runtime_input'
			                 AND pending.kind = 'runtime_input'
			                 AND (
			                      pending.priority > candidate.priority
			                      OR (
			                         pending.priority = candidate.priority
			                         AND pending.queue_partition_sequence < candidate.queue_partition_sequence
			                      )
			                 )
			                 AND NOT EXISTS (
			                     SELECT 1
			                       FROM queue_jobs barrier
			                      WHERE barrier.workspace_id = candidate.workspace_id
			                        AND barrier.partition_key = candidate.partition_key
			                        AND barrier.status = 'pending'
			                        AND barrier.kind <> 'runtime_input'
			                        AND NOT (
			                            candidate.payload_json::jsonb ->> 'input_kind' = 'interrupt_control'
			                            AND barrier.kind = 'runtime_config_update'
			                        )
			                        AND (
			                            barrier.available_at <= $2
			                            OR (
			                                candidate.payload_json::jsonb ->> 'input_kind' IS DISTINCT FROM 'interrupt_control'
			                                AND barrier.kind = 'runtime_config_update'
			                            )
			                        )
			                        AND barrier.queue_partition_sequence < pending.queue_partition_sequence
			                 )
			                )
			                OR (candidate.kind = 'runtime_input'
			                    AND pending.kind <> 'runtime_input'
			                    AND NOT (
			                        candidate.payload_json::jsonb ->> 'input_kind' = 'interrupt_control'
			                        AND pending.kind = 'runtime_config_update'
			                    )
			                    AND pending.queue_partition_sequence < candidate.queue_partition_sequence)
			                OR (candidate.kind <> 'runtime_input'
			                    AND pending.queue_partition_sequence < candidate.queue_partition_sequence)
			           )
			    )
			  ORDER BY candidate.priority DESC, candidate.available_at ASC,
			           candidate.partition_key ASC, candidate.queue_partition_sequence ASC
			  FOR UPDATE SKIP LOCKED
			  LIMIT $3`
		queryArgs := append([]any{
			string(request.WorkspaceID),
			request.Now,
			request.MaxJobs * 8,
		}, args...)
		rows, err := tx.Query(ctx, query, queryArgs...)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		var candidates []leaseCandidateRow
		for rows.Next() {
			var candidate leaseCandidateRow
			if err := rows.Scan(
				&candidate.id,
				&candidate.partitionKey,
				&candidate.kind,
				&candidate.priority,
				&candidate.availableAt,
				&candidate.partitionSequence,
				&candidate.inputKind,
			); err != nil {
				return err
			}
			candidates = append(candidates, candidate)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		for _, candidate := range candidates {
			if len(leased) >= request.MaxJobs {
				break
			}
			leaseToken, err := NewLeaseToken()
			if err != nil {
				return err
			}
			job, updated, err := leaseCandidate(ctx, tx, request, candidate, leaseToken, s.retryPolicy.MaxAttempts)
			if err != nil {
				return err
			}
			if updated {
				leased = append(leased, job)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return leased, nil
}

func leaseKindPredicate(kinds []string, placeholderStart int) (string, []any) {
	parts := make([]string, 0, len(kinds))
	args := make([]any, 0, len(kinds))
	for index, kind := range kinds {
		parts = append(parts, "$"+strconv.Itoa(placeholderStart+index))
		args = append(args, kind)
	}
	return strings.Join(parts, ", "), args
}

type leaseCandidateRow struct {
	id                string
	partitionKey      string
	kind              string
	priority          int
	availableAt       time.Time
	partitionSequence int64
	inputKind         string
}

func leaseCandidate(ctx context.Context, tx *dbconnect.Tx, request LeaseRequest, candidate leaseCandidateRow, leaseToken string, defaultMaxAttempts int) (*Job, bool, error) {
	leasedAt := request.Now
	leasedUntil := request.Now.Add(request.LeaseDuration)
	row := tx.QueryRow(ctx,
		`UPDATE queue_jobs
		    SET status = 'leased',
		        leased_by = $5,
		        lease_token = $6,
		        leased_at = $7,
		        leased_until = $8,
		        attempt_count = attempt_count + 1,
		        updated_at = $7
		  WHERE workspace_id = $1
		    AND id = $2
		    AND kind = $3
		    AND status = 'pending'
		    AND available_at = $9
		    AND queue_partition_sequence = $10
		    AND NOT EXISTS (
		        SELECT 1
		          FROM queue_jobs leased
		         WHERE leased.workspace_id = $1
		           AND leased.partition_key = $4
		           AND leased.status = 'leased'
		    )
		    AND NOT EXISTS (
		        SELECT 1
		          FROM queue_jobs pending
		         WHERE pending.workspace_id = $1
		           AND pending.partition_key = $4
		           AND pending.status = 'pending'
		           AND pending.id <> $2
		           AND (
		                pending.available_at <= $7
		                OR (
		                    $3 = 'runtime_input'
		                    AND $12 <> 'interrupt_control'
		                    AND pending.kind = 'runtime_config_update'
		                    AND pending.queue_partition_sequence < $10
		                )
		           )
		           AND (
		                ($3 = 'runtime_input'
		                 AND pending.kind = 'runtime_input'
		                 AND (
		                      pending.priority > $11
		                      OR (
		                         pending.priority = $11
		                         AND pending.queue_partition_sequence < $10
		                      )
		                 )
		                 AND NOT EXISTS (
		                     SELECT 1
		                       FROM queue_jobs barrier
		                      WHERE barrier.workspace_id = $1
		                        AND barrier.partition_key = $4
		                        AND barrier.status = 'pending'
		                        AND barrier.kind <> 'runtime_input'
		                        AND NOT (
		                            $12 = 'interrupt_control'
		                            AND barrier.kind = 'runtime_config_update'
		                        )
		                        AND (
		                            barrier.available_at <= $7
		                            OR (
		                                $12 <> 'interrupt_control'
		                                AND barrier.kind = 'runtime_config_update'
		                            )
		                        )
		                        AND barrier.queue_partition_sequence < pending.queue_partition_sequence
		                 )
		                )
		                OR ($3 = 'runtime_input'
		                    AND pending.kind <> 'runtime_input'
		                    AND NOT (
		                        $12 = 'interrupt_control'
		                        AND pending.kind = 'runtime_config_update'
		                    )
		                    AND pending.queue_partition_sequence < $10)
		                OR ($3 <> 'runtime_input'
		                    AND pending.queue_partition_sequence < $10)
		           )
		    )
		  RETURNING id, workspace_id, kind, partition_key, queue_partition_sequence, dedupe_key, payload_version,
		            status, payload_json, priority, available_at, leased_by, lease_token,
		            leased_at, leased_until, attempt_count, max_attempts, created_at, updated_at`,
		string(request.WorkspaceID),
		candidate.id,
		candidate.kind,
		candidate.partitionKey,
		request.LeaseOwner,
		leaseToken,
		leasedAt,
		leasedUntil,
		candidate.availableAt,
		candidate.partitionSequence,
		candidate.priority,
		candidate.inputKind,
	)
	job, err := scanJob(row)
	if dbconnect.IsNoRows(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if job.MaxAttempts == 0 {
		job.MaxAttempts = defaultMaxAttempts
	}
	return job, true, nil
}

func (s *PostgreSQLQueueStore) ReclaimExpiredLeases(ctx context.Context, request ReclaimExpiredLeasesRequest) (int, error) {
	if err := validateReclaimExpiredLeasesRequest(request); err != nil {
		return 0, err
	}
	if s == nil || s.client == nil {
		return 0, &ValidationError{Message: "queue store is required"}
	}
	if request.Now.IsZero() {
		request.Now = storage.Now()
	}
	if request.Limit == 0 {
		request.Limit = 100
	}
	now := request.Now.UTC()
	reclaimed := 0
	reclaim := func(tx *dbconnect.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT id, workspace_id
			   FROM queue_jobs
			  WHERE ($1 = '' OR workspace_id = $1)
			    AND status = 'leased'
			    AND leased_until <= $2
			    AND ($3 = '' OR kind = $3)
			  ORDER BY leased_until ASC, id ASC
			  FOR UPDATE SKIP LOCKED
			  LIMIT $4`,
			string(request.WorkspaceID),
			now,
			request.Kind,
			request.Limit,
		)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		type expiredLease struct {
			id          string
			workspaceID string
		}
		var expired []expiredLease
		for rows.Next() {
			var lease expiredLease
			if err := rows.Scan(&lease.id, &lease.workspaceID); err != nil {
				return err
			}
			expired = append(expired, lease)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		for _, lease := range expired {
			result, err := tx.Exec(ctx,
				`UPDATE queue_jobs
				    SET status = 'pending',
				        available_at = $3,
				        lease_token = NULL,
				        leased_by = NULL,
				        leased_at = NULL,
				        leased_until = NULL,
				        last_error_kind = $4,
				        last_error_message = $5,
				        updated_at = $3
				  WHERE workspace_id = $1
				    AND id = $2
				    AND status = 'leased'`,
				lease.workspaceID,
				lease.id,
				now,
				nullableString(defaultString(request.ErrorKind, "lease_expired")),
				nullableString(defaultString(request.ErrorMessage, "queue lease expired")),
			)
			if err != nil {
				return err
			}
			if rowsAffected(result) {
				reclaimed++
			}
		}
		return nil
	}
	var err error
	if request.WorkspaceID == "" {
		err = s.client.WithTx(ctx, "queue.reclaim_expired_leases", nil, func(tx *dbconnect.Tx) error {
			if _, err := tx.Exec(ctx, "SELECT set_config('tetral.queue_maintenance', 'true', true)"); err != nil {
				return err
			}
			return reclaim(tx)
		})
	} else {
		err = s.client.WithWorkspaceTx(ctx, string(request.WorkspaceID), "queue.reclaim_expired_leases", reclaim)
	}
	if err != nil {
		return 0, err
	}
	return reclaimed, nil
}

// ListPendingAtOrOverBudget returns nonlocking references to Sandbox jobs
// whose explicit attempt budget was spent before a crashed lease was
// reclaimed. Callers must recheck the observation in their business
// transaction before changing either business state or the Queue row.
func (s *PostgreSQLQueueStore) ListPendingAtOrOverBudget(ctx context.Context, request ListPendingAtOrOverBudgetRequest) ([]PendingAtOrOverBudgetJob, error) {
	if request.Limit < 0 {
		return nil, &ValidationError{Message: "limit must not be negative"}
	}
	if s == nil || s.client == nil {
		return nil, &ValidationError{Message: "queue store is required"}
	}
	request.Limit = sandboxMaintenanceLimit(request.Limit)
	var jobs []PendingAtOrOverBudgetJob
	err := s.client.WithTx(ctx, "queue.list_pending_at_or_over_budget", nil, func(tx *dbconnect.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT set_config('tetral.queue_maintenance', 'true', true)"); err != nil {
			return err
		}
		var invalidBudget bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (
			    SELECT 1
			      FROM queue_jobs
			     WHERE kind IN (`+sandboxJobKindSQLList+`)
			       AND status = 'pending'
			       AND max_attempts = 0
			)`,
		).Scan(&invalidBudget); err != nil {
			return err
		}
		if invalidBudget {
			return &IntegrityError{Message: "sandbox queue job is missing its explicit attempt budget"}
		}
		rows, err := tx.Query(ctx,
			`SELECT workspace_id, id, kind, partition_key, dedupe_key,
			        payload_json, attempt_count, max_attempts
			   FROM queue_jobs
			  WHERE kind IN (`+sandboxJobKindSQLList+`)
			    AND status = 'pending'
			    AND max_attempts > 0
			    AND attempt_count >= max_attempts
			  ORDER BY updated_at ASC, id ASC
			  LIMIT $1`,
			request.Limit,
		)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var job PendingAtOrOverBudgetJob
			var workspaceID string
			var payload string
			if err := rows.Scan(
				&workspaceID,
				&job.JobID,
				&job.Kind,
				&job.PartitionKey,
				&job.DedupeKey,
				&payload,
				&job.AttemptCount,
				&job.MaxAttempts,
			); err != nil {
				return err
			}
			job.WorkspaceID = workspace.ID(workspaceID)
			job.PayloadJSON = append(json.RawMessage(nil), payload...)
			jobs = append(jobs, job)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

// SweepSandboxTerminalJobs removes only old terminal notifications owned by
// the Sandbox subsystem. Durable Sandbox business records are unaffected.
func (s *PostgreSQLQueueStore) SweepSandboxTerminalJobs(ctx context.Context, request SandboxTerminalSweepRequest) (int, error) {
	if request.Limit < 0 {
		return 0, &ValidationError{Message: "limit must not be negative"}
	}
	if s == nil || s.client == nil {
		return 0, &ValidationError{Message: "queue store is required"}
	}
	if request.Now.IsZero() {
		request.Now = storage.Now()
	}
	request.Limit = sandboxMaintenanceLimit(request.Limit)
	deleted := 0
	err := s.client.WithTx(ctx, "queue.sweep_sandbox_terminal_jobs", nil, func(tx *dbconnect.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT set_config('tetral.queue_maintenance', 'true', true)"); err != nil {
			return err
		}
		var invalidTerminalTimestamp bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (
			    SELECT 1
			      FROM queue_jobs
			     WHERE kind IN (`+sandboxJobKindSQLList+`)
			       AND (
			            (status = 'acknowledged' AND acknowledged_at IS NULL)
			            OR (status = 'cancelled' AND cancelled_at IS NULL)
			            OR (status = 'dead_lettered' AND dead_lettered_at IS NULL)
			       )
			)`,
		).Scan(&invalidTerminalTimestamp); err != nil {
			return err
		}
		if invalidTerminalTimestamp {
			return &IntegrityError{Message: "sandbox terminal queue job is missing its status timestamp"}
		}
		rows, err := tx.Query(ctx,
			`WITH candidates AS (
			    SELECT workspace_id, id
			      FROM queue_jobs
			     WHERE kind IN (`+sandboxJobKindSQLList+`)
			       AND (
			            (status = 'acknowledged' AND acknowledged_at <= $1)
			            OR (status = 'cancelled' AND cancelled_at <= $1)
			            OR (status = 'dead_lettered' AND dead_lettered_at <= $1)
			       )
			     ORDER BY COALESCE(acknowledged_at, cancelled_at, dead_lettered_at) ASC, id ASC
			     FOR UPDATE SKIP LOCKED
			     LIMIT $2
			), deleted AS (
			    DELETE FROM queue_jobs jobs
			     USING candidates
			     WHERE jobs.workspace_id = candidates.workspace_id
			       AND jobs.id = candidates.id
			     RETURNING jobs.id
			)
			SELECT COUNT(*) FROM deleted`,
			request.Now.UTC().Add(-SandboxTerminalRetentionAge),
			request.Limit,
		)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		if !rows.Next() {
			return rows.Err()
		}
		return rows.Scan(&deleted)
	})
	if err != nil {
		return 0, err
	}
	return deleted, nil
}

// SweepEmptyPartitionCounters removes counters only after every Queue job in
// the partition has gone. The locked-row recheck prevents deletion from
// racing a concurrent enqueue that uses the same counter.
func (s *PostgreSQLQueueStore) SweepEmptyPartitionCounters(ctx context.Context, request EmptyPartitionCounterSweepRequest) (int, error) {
	if request.Limit < 0 {
		return 0, &ValidationError{Message: "limit must not be negative"}
	}
	if s == nil || s.client == nil {
		return 0, &ValidationError{Message: "queue store is required"}
	}
	request.Limit = sandboxMaintenanceLimit(request.Limit)
	deleted := 0
	err := s.client.WithTx(ctx, "queue.sweep_empty_partition_counters", nil, func(tx *dbconnect.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT set_config('tetral.queue_maintenance', 'true', true)"); err != nil {
			return err
		}
		rows, err := tx.Query(ctx,
			`SELECT counters.workspace_id, counters.partition_key
			   FROM queue_partition_counters counters
			  WHERE NOT EXISTS (
			        SELECT 1
			          FROM queue_jobs jobs
			         WHERE jobs.workspace_id = counters.workspace_id
			           AND jobs.partition_key = counters.partition_key
			  )
			  ORDER BY counters.workspace_id ASC, counters.partition_key ASC
			  LIMIT $1`,
			request.Limit,
		)
		if err != nil {
			return err
		}
		type counterKey struct {
			workspaceID  string
			partitionKey string
		}
		var candidates []counterKey
		for rows.Next() {
			var candidate counterKey
			if err := rows.Scan(&candidate.workspaceID, &candidate.partitionKey); err != nil {
				_ = rows.Close()
				return err
			}
			candidates = append(candidates, candidate)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, candidate := range candidates {
			var lockedPartitionKey string
			if err := tx.QueryRow(ctx,
				`SELECT partition_key
				   FROM queue_partition_counters
				  WHERE workspace_id = $1 AND partition_key = $2
				  FOR UPDATE`,
				candidate.workspaceID,
				candidate.partitionKey,
			).Scan(&lockedPartitionKey); dbconnect.IsNoRows(err) {
				continue
			} else if err != nil {
				return err
			}
			result, err := tx.Exec(ctx,
				`DELETE FROM queue_partition_counters counters
				  WHERE counters.workspace_id = $1
				    AND counters.partition_key = $2
				    AND NOT EXISTS (
				        SELECT 1
				          FROM queue_jobs jobs
				         WHERE jobs.workspace_id = counters.workspace_id
				           AND jobs.partition_key = counters.partition_key
				    )`,
				candidate.workspaceID,
				candidate.partitionKey,
			)
			if err != nil {
				return err
			}
			deleted += affectedCount(result)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return deleted, nil
}

func (s *PostgreSQLQueueStore) Heartbeat(ctx context.Context, request HeartbeatRequest) (bool, error) {
	if err := validateFencedRequest(request.WorkspaceID, request.JobID, request.LeaseToken); err != nil {
		return false, err
	}
	if request.LeaseDuration <= 0 {
		return false, &ValidationError{Message: "lease_duration must be positive"}
	}
	if request.Now.IsZero() {
		request.Now = storage.Now()
	}
	if s == nil || s.client == nil {
		return false, &ValidationError{Message: "queue store is required"}
	}
	var updated bool
	if err := s.client.WithWorkspaceTx(ctx, string(request.WorkspaceID), "queue.heartbeat", func(tx *dbconnect.Tx) error {
		result, err := tx.Exec(ctx,
			`UPDATE queue_jobs
			    SET leased_until = $4,
			        updated_at = $5
			  WHERE workspace_id = $1
			    AND id = $2
			    AND lease_token = $3
			    AND status = 'leased'`,
			string(request.WorkspaceID),
			request.JobID,
			request.LeaseToken,
			request.Now.UTC().Add(request.LeaseDuration),
			request.Now.UTC(),
		)
		if err != nil {
			return err
		}
		updated = rowsAffected(result)
		return nil
	}); err != nil {
		return false, err
	}
	return updated, nil
}

func (s *PostgreSQLQueueStore) Ack(ctx context.Context, request AckRequest) (bool, error) {
	if err := validateFencedRequest(request.WorkspaceID, request.JobID, request.LeaseToken); err != nil {
		return false, err
	}
	if request.Now.IsZero() {
		request.Now = storage.Now()
	}
	if s == nil || s.client == nil {
		return false, &ValidationError{Message: "queue store is required"}
	}
	var updated bool
	err := s.client.WithWorkspaceTx(ctx, string(request.WorkspaceID), "queue.acknowledged", func(tx *dbconnect.Tx) error {
		var err error
		updated, err = AckTx(ctx, tx, request)
		return err
	})
	return updated, err
}

// AckTx acknowledges one leased job inside a caller-owned business
// transaction. It is used when durable business state and removal of the
// current Queue notification must become visible together.
func AckTx(ctx context.Context, tx queueMutationTransaction, request AckRequest) (bool, error) {
	if err := validateFencedRequest(request.WorkspaceID, request.JobID, request.LeaseToken); err != nil {
		return false, err
	}
	if tx == nil {
		return false, &ValidationError{Message: "queue transaction is required"}
	}
	if request.Now.IsZero() {
		request.Now = storage.Now()
	}
	result, err := tx.Exec(ctx,
		`UPDATE queue_jobs
		    SET status = 'acknowledged', acknowledged_at = $4,
		        lease_token = NULL, leased_by = NULL, leased_at = NULL,
		        leased_until = NULL, last_error_kind = NULL,
		        last_error_message = NULL, updated_at = $4
		  WHERE workspace_id = $1 AND id = $2 AND lease_token = $3 AND status = 'leased'`,
		string(request.WorkspaceID), request.JobID, request.LeaseToken, request.Now.UTC(),
	)
	if err != nil {
		return false, err
	}
	return rowsAffected(result), nil
}

func (s *PostgreSQLQueueStore) Retry(ctx context.Context, request RetryRequest) (bool, error) {
	if err := validateFencedRequest(request.WorkspaceID, request.JobID, request.LeaseToken); err != nil {
		return false, err
	}
	if request.Now.IsZero() {
		request.Now = storage.Now()
	}
	if s == nil || s.client == nil {
		return false, &ValidationError{Message: "queue store is required"}
	}
	var updated bool
	err := s.client.WithWorkspaceTx(ctx, string(request.WorkspaceID), "queue.retry", func(tx *dbconnect.Tx) error {
		var attemptCount int
		var maxAttempts int
		if err := tx.QueryRow(ctx,
			`SELECT attempt_count, max_attempts
			   FROM queue_jobs
			  WHERE workspace_id = $1
			    AND id = $2
			    AND lease_token = $3
			    AND status = 'leased'
			  FOR UPDATE`,
			string(request.WorkspaceID),
			request.JobID,
			request.LeaseToken,
		).Scan(&attemptCount, &maxAttempts); dbconnect.IsNoRows(err) {
			return nil
		} else if err != nil {
			return err
		}
		now := request.Now.UTC()
		effectiveMaxAttempts := maxAttempts
		if effectiveMaxAttempts == 0 {
			effectiveMaxAttempts = s.retryPolicy.MaxAttempts
		}
		if attemptCount >= effectiveMaxAttempts {
			result, err := tx.Exec(ctx,
				`UPDATE queue_jobs
				    SET status = 'dead_lettered',
				        dead_lettered_at = $4,
				        lease_token = NULL,
				        leased_by = NULL,
				        leased_at = NULL,
				        leased_until = NULL,
				        last_error_kind = $5,
				        last_error_message = $6,
				        updated_at = $4
				  WHERE workspace_id = $1
				    AND id = $2
				    AND lease_token = $3
				    AND status = 'leased'`,
				string(request.WorkspaceID),
				request.JobID,
				request.LeaseToken,
				now,
				nullableString(request.ErrorKind),
				nullableString(request.ErrorMessage),
			)
			if err != nil {
				return err
			}
			updated = rowsAffected(result)
			return nil
		}
		retryDelay := queueRetryDelay(s.retryPolicy, attemptCount)
		result, err := tx.Exec(ctx,
			`UPDATE queue_jobs
			    SET status = 'pending',
			        available_at = $4,
			        lease_token = NULL,
			        leased_by = NULL,
			        leased_at = NULL,
			        leased_until = NULL,
			        last_error_kind = $5,
			        last_error_message = $6,
			        updated_at = $7
			  WHERE workspace_id = $1
			    AND id = $2
			    AND lease_token = $3
			    AND status = 'leased'`,
			string(request.WorkspaceID),
			request.JobID,
			request.LeaseToken,
			now.Add(retryDelay),
			nullableString(request.ErrorKind),
			nullableString(request.ErrorMessage),
			now,
		)
		if err != nil {
			return err
		}
		updated = rowsAffected(result)
		return nil
	})
	if err != nil {
		return false, err
	}
	return updated, nil
}

func (s *PostgreSQLQueueStore) Defer(ctx context.Context, request DeferRequest) (bool, error) {
	if err := validateFencedRequest(request.WorkspaceID, request.JobID, request.LeaseToken); err != nil {
		return false, err
	}
	if request.Now.IsZero() {
		request.Now = storage.Now()
	}
	if s == nil || s.client == nil {
		return false, &ValidationError{Message: "queue store is required"}
	}
	var updated bool
	err := s.client.WithWorkspaceTx(ctx, string(request.WorkspaceID), "queue.defer", func(tx *dbconnect.Tx) error {
		var kind string
		var partitionKey string
		var dedupeKey sql.NullString
		var payloadVersion int
		var payloadJSON string
		var attemptCount int
		if err := tx.QueryRow(ctx,
			`SELECT kind, partition_key, dedupe_key, payload_version, payload_json, attempt_count
			   FROM queue_jobs
			  WHERE workspace_id = $1
			    AND id = $2
			    AND lease_token = $3
			    AND status = 'leased'
			  FOR UPDATE`,
			string(request.WorkspaceID),
			request.JobID,
			request.LeaseToken,
		).Scan(&kind, &partitionKey, &dedupeKey, &payloadVersion, &payloadJSON, &attemptCount); dbconnect.IsNoRows(err) {
			return nil
		} else if err != nil {
			return err
		}
		if kind != KindRuntimeConfigUpdate {
			return &ValidationError{Message: "queue job kind cannot be deferred"}
		}
		if attemptCount < 1 {
			return errors.New("queue: leased job has invalid attempt count")
		}
		now := request.Now.UTC()
		if kind == KindRuntimeConfigUpdate {
			if err := validateCanonicalQueueShape(EnqueueRequest{
				ID:             request.JobID,
				WorkspaceID:    request.WorkspaceID,
				Kind:           kind,
				PartitionKey:   partitionKey,
				DedupeKey:      dedupeKey.String,
				PayloadVersion: payloadVersion,
				PayloadJSON:    json.RawMessage(payloadJSON),
			}); err != nil {
				return err
			}
			var deferCount int
			if err := tx.QueryRow(ctx,
				`SELECT defer_count
				   FROM queue_jobs
				  WHERE workspace_id = $1
				    AND id = $2
				    AND lease_token = $3
				    AND status = 'leased'`,
				string(request.WorkspaceID),
				request.JobID,
				request.LeaseToken,
			).Scan(&deferCount); err != nil {
				return err
			}
			deferDelay := queueRetryDelay(s.retryPolicy, deferCount+1)
			result, err := tx.Exec(ctx,
				`UPDATE queue_jobs
				    SET status = 'pending',
				        available_at = $4,
				        attempt_count = attempt_count - 1,
				        defer_count = defer_count + 1,
				        lease_token = NULL,
				        leased_by = NULL,
				        leased_at = NULL,
				        leased_until = NULL,
				        updated_at = $5
				  WHERE workspace_id = $1
				    AND id = $2
				    AND lease_token = $3
				    AND status = 'leased'`,
				string(request.WorkspaceID),
				request.JobID,
				request.LeaseToken,
				now.Add(deferDelay),
				now,
			)
			if err != nil {
				return err
			}
			updated = rowsAffected(result)
			return nil
		}
		deferDelay := queueRetryDelay(s.retryPolicy, attemptCount)
		result, err := tx.Exec(ctx,
			`UPDATE queue_jobs
			    SET status = 'pending',
			        available_at = $4,
			        attempt_count = attempt_count - 1,
			        lease_token = NULL,
			        leased_by = NULL,
			        leased_at = NULL,
			        leased_until = NULL,
			        updated_at = $5
			  WHERE workspace_id = $1
			    AND id = $2
			    AND lease_token = $3
			    AND status = 'leased'`,
			string(request.WorkspaceID),
			request.JobID,
			request.LeaseToken,
			now.Add(deferDelay),
			now,
		)
		if err != nil {
			return err
		}
		updated = rowsAffected(result)
		return nil
	})
	if err != nil {
		return false, err
	}
	return updated, nil
}

// queueRetryDelay is the Queue-owned retry/defer backoff. Retry carries no
// client delay authority: RetryRequest reports only the failure (ErrorKind,
// ErrorMessage) and has no retry_after field, so the durable available_at is
// derived here from the transition-owned counter as capped exponential backoff
// with full jitter. Retry supplies attempt_count; runtime_config_update Defer
// supplies its scoped defer_count. The doubling
// loop yields capDelay = min(MaxDelay, BaseDelay*2^(count-1)), saturating at MaxDelay, and the
// returned delay is uniform in [0, capDelay] (RandomInt64(capDelay+1) makes the
// cap inclusive). The transitions share this computation; the RetryPolicy inputs
// (BaseDelay 1s, MaxDelay 1m, MaxAttempts DefaultMaxAttempts) are filled by
// normalizeRetryPolicy.
//
// The Retry caller reschedules with this delay only while budget remains; once
// the leased row's attempt_count reaches the effective max_attempts (the row's
// max_attempts, or the policy default when the stored value is 0) the job goes
// to dead_lettered instead, so a persistently failing job stops looping.
//
// UPDATE-WITH: the Retry and Defer transitions in this file, and RetryPolicy /
// normalizeRetryPolicy.
func queueRetryDelay(policy RetryPolicy, attemptCount int) time.Duration {
	capDelay := policy.BaseDelay
	for attempt := 1; attempt < attemptCount && capDelay < policy.MaxDelay; attempt++ {
		if capDelay > policy.MaxDelay/2 {
			capDelay = policy.MaxDelay
			break
		}
		capDelay *= 2
	}
	if capDelay > policy.MaxDelay {
		capDelay = policy.MaxDelay
	}
	return time.Duration(policy.RandomInt64(int64(capDelay) + 1))
}

func (s *PostgreSQLQueueStore) DeadLetter(ctx context.Context, request DeadLetterRequest) (bool, error) {
	if err := validateFencedRequest(request.WorkspaceID, request.JobID, request.LeaseToken); err != nil {
		return false, err
	}
	if request.Now.IsZero() {
		request.Now = storage.Now()
	}
	return s.fencedTerminalUpdate(ctx, request.WorkspaceID, request.JobID, request.LeaseToken, StatusDeadLettered, "dead_lettered_at", request.ErrorKind, request.ErrorMessage, request.Now.UTC())
}

func (s *PostgreSQLQueueStore) Cancel(ctx context.Context, request CancelRequest) (int, error) {
	if err := validateCancelRequest(request); err != nil {
		return 0, err
	}
	if request.Now.IsZero() {
		request.Now = storage.Now()
	}
	if s == nil || s.client == nil {
		return 0, &ValidationError{Message: "queue store is required"}
	}
	var cancelled int
	if err := s.client.WithWorkspaceTx(ctx, string(request.WorkspaceID), "queue.cancel", func(tx *dbconnect.Tx) error {
		result, err := tx.Exec(ctx,
			`UPDATE queue_jobs
			    SET status = 'cancelled',
			        cancelled_at = $3,
			        updated_at = $3
			  WHERE workspace_id = $1
			    AND partition_key = $2
			    AND status = 'pending'
			    AND kind = 'runtime_input'
			    AND payload_json::jsonb ->> 'session_thread_id' = $4
			    AND payload_json::jsonb ->> 'input_kind' = 'messages'
			    AND (payload_json::jsonb ->> 'sequence_to')::bigint < $5`,
			string(request.WorkspaceID),
			FormatSessionPartitionKey(request.WorkspaceID, request.SessionID),
			request.Now.UTC(),
			request.SessionThreadID,
			request.InterruptFenceSequence,
		)
		if err != nil {
			return err
		}
		cancelled = affectedCount(result)
		return nil
	}); err != nil {
		return 0, err
	}
	return cancelled, nil
}

func (s *PostgreSQLQueueStore) fencedTerminalUpdate(ctx context.Context, workspaceID workspace.ID, jobID string, leaseToken string, status string, timestampColumn string, errorKind string, errorMessage string, now time.Time) (bool, error) {
	if s == nil || s.client == nil {
		return false, &ValidationError{Message: "queue store is required"}
	}
	query := `UPDATE queue_jobs
	    SET status = '` + status + `',
	        ` + timestampColumn + ` = $4,
	        lease_token = NULL,
	        leased_by = NULL,
	        leased_at = NULL,
	        leased_until = NULL,
	        last_error_kind = $5,
	        last_error_message = $6,
	        updated_at = $4
	  WHERE workspace_id = $1
	    AND id = $2
	    AND lease_token = $3
	    AND status = 'leased'`
	var updated bool
	if err := s.client.WithWorkspaceTx(ctx, string(workspaceID), "queue."+status, func(tx *dbconnect.Tx) error {
		result, err := tx.Exec(ctx,
			query,
			string(workspaceID),
			jobID,
			leaseToken,
			now,
			nullableString(errorKind),
			nullableString(errorMessage),
		)
		if err != nil {
			return err
		}
		updated = rowsAffected(result)
		return nil
	}); err != nil {
		return false, err
	}
	return updated, nil
}

func scanJob(row interface{ Scan(dest ...any) error }) (*Job, error) {
	var job Job
	var workspaceID string
	var dedupeKey sql.NullString
	var payload string
	var availableAt time.Time
	var leasedBy sql.NullString
	var leaseToken sql.NullString
	var leasedAt sql.NullTime
	var leasedUntil sql.NullTime
	var createdAt time.Time
	var updatedAt time.Time
	if err := row.Scan(
		&job.ID,
		&workspaceID,
		&job.Kind,
		&job.PartitionKey,
		&job.QueuePartitionSequence,
		&dedupeKey,
		&job.PayloadVersion,
		&job.Status,
		&payload,
		&job.Priority,
		&availableAt,
		&leasedBy,
		&leaseToken,
		&leasedAt,
		&leasedUntil,
		&job.AttemptCount,
		&job.MaxAttempts,
		&createdAt,
		&updatedAt,
	); err != nil {
		return nil, err
	}
	job.WorkspaceID = workspace.ID(workspaceID)
	job.DedupeKey = dedupeKey.String
	job.PayloadJSON = append(json.RawMessage(nil), payload...)
	job.AvailableAt = availableAt.UTC()
	job.LeasedBy = leasedBy.String
	job.LeaseToken = leaseToken.String
	if leasedAt.Valid {
		value := leasedAt.Time.UTC()
		job.LeasedAt = &value
	}
	if leasedUntil.Valid {
		value := leasedUntil.Time.UTC()
		job.LeasedUntil = &value
	}
	job.CreatedAt = createdAt.UTC()
	job.UpdatedAt = updatedAt.UTC()
	return &job, nil
}

func rowsAffected(result sql.Result) bool {
	return affectedCount(result) > 0
}

func affectedCount(result sql.Result) int {
	count, err := result.RowsAffected()
	if err != nil || count <= 0 {
		return 0
	}
	return int(count)
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func defaultString(value string, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func sandboxMaintenanceLimit(limit int) int {
	if limit <= 0 || limit > SandboxMaintenanceBatchLimit {
		return SandboxMaintenanceBatchLimit
	}
	return limit
}
