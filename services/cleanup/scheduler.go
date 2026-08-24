package tetralcleanup

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	ServiceName = "cleanup"

	defaultClaimLimit = 100
)

type Scheduler struct {
	Client     *dbconnect.Client
	Clock      func() time.Time
	IDStrategy func(string) string
}

type ClaimDueRequest struct {
	WorkspaceID workspace.ID
	Limit       int
}

type ClaimedCleanupJob struct {
	WorkspaceID  workspace.ID
	SessionID    string
	CleanupJobID string
	QueueJobID   string
}

func NewScheduler(client *dbconnect.Client) *Scheduler {
	return &Scheduler{
		Client:     client,
		Clock:      func() time.Time { return storage.Now() },
		IDStrategy: id.New,
	}
}

func (s *Scheduler) ClaimDue(ctx context.Context, request ClaimDueRequest) ([]ClaimedCleanupJob, error) {
	if s == nil || s.Client == nil {
		return nil, &ValidationError{Message: "cleanup scheduler store is required"}
	}
	if request.WorkspaceID == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	limit := request.Limit
	if limit <= 0 {
		limit = defaultClaimLimit
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	var claimed []ClaimedCleanupJob
	err := s.Client.WithWorkspaceTx(ctx, string(request.WorkspaceID), "tetralcleanup.claim_due", func(tx *dbconnect.Tx) error {
		sessionIDs, err := dueCleanupSessionsTx(ctx, tx, request.WorkspaceID, now, limit)
		if err != nil {
			return err
		}
		sort.Strings(sessionIDs)
		type cleanupEnqueue struct {
			sessionID    string
			cleanupJobID string
			request      queue.EnqueueRequest
		}
		enqueues := make([]cleanupEnqueue, 0, len(sessionIDs))
		for _, sessionID := range sessionIDs {
			if err := storage.AcquireSessionRuntimeMutationLock(ctx, tx, string(request.WorkspaceID), sessionID); err != nil {
				return err
			}
			cleanupJobID := s.newID("cleanup_")
			queueJobID := s.newID(queue.JobIDPrefix)
			updated, err := markCleanupEnqueuedTx(ctx, tx, request.WorkspaceID, sessionID, cleanupJobID, now)
			if err != nil {
				return err
			}
			if !updated {
				continue
			}
			payload, err := cleanupQueuePayload(request.WorkspaceID, sessionID, cleanupJobID)
			if err != nil {
				return err
			}
			enqueues = append(enqueues, cleanupEnqueue{
				sessionID:    sessionID,
				cleanupJobID: cleanupJobID,
				request: queue.EnqueueRequest{
					ID:             queueJobID,
					WorkspaceID:    request.WorkspaceID,
					Kind:           queue.KindCleanupSession,
					PartitionKey:   queue.FormatSessionPartitionKey(request.WorkspaceID, sessionID),
					DedupeKey:      queue.FormatCleanupSessionDedupeKey(request.WorkspaceID, sessionID, cleanupJobID),
					PayloadVersion: 1,
					PayloadJSON:    payload,
					AvailableAt:    now,
					Now:            now,
				},
			})
		}
		requests := make([]queue.EnqueueRequest, len(enqueues))
		for index := range enqueues {
			requests[index] = enqueues[index].request
		}
		jobs, err := queue.EnqueueBatchTx(ctx, tx, requests)
		if err != nil {
			return err
		}
		for index, enqueue := range enqueues {
			claimed = append(claimed, ClaimedCleanupJob{
				WorkspaceID:  request.WorkspaceID,
				SessionID:    enqueue.sessionID,
				CleanupJobID: enqueue.cleanupJobID,
				QueueJobID:   jobs[index].ID,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return claimed, nil
}

func (s *Scheduler) newID(prefix string) string {
	if s != nil && s.IDStrategy != nil {
		return s.IDStrategy(prefix)
	}
	return id.New(prefix)
}

func dueCleanupSessionsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID workspace.ID, now time.Time, limit int) ([]string, error) {
	rows, err := tx.Query(ctx,
		`SELECT session_id
		   FROM session_runtime_status
		  WHERE workspace_id = $1
		    AND status = 'idle'
		    AND cleanup_after <= $2
		    AND cleanup_job_id IS NULL
		    AND binding_id IS NOT NULL
		  ORDER BY cleanup_after ASC, session_id ASC
		  LIMIT $3`,
		string(workspaceID),
		now,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var sessionIDs []string
	for rows.Next() {
		var sessionID string
		if err := rows.Scan(&sessionID); err != nil {
			return nil, err
		}
		sessionIDs = append(sessionIDs, sessionID)
	}
	return sessionIDs, rows.Err()
}

func markCleanupEnqueuedTx(ctx context.Context, tx *dbconnect.Tx, workspaceID workspace.ID, sessionID string, cleanupJobID string, now time.Time) (bool, error) {
	result, err := tx.Exec(ctx,
		`UPDATE session_runtime_status
		    SET cleanup_job_id = $4,
		        cleanup_enqueued_at = $5,
		        cleanup_claimed_at = NULL,
		        updated_at = $5
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND status = 'idle'
		    AND cleanup_job_id IS NULL
		    AND binding_id IS NOT NULL
		    AND cleanup_after IS NOT NULL
		    AND cleanup_after <= $3`,
		string(workspaceID),
		sessionID,
		now,
		cleanupJobID,
		now,
	)
	if err != nil {
		return false, err
	}
	return rowsAffected(result), nil
}

func cleanupQueuePayload(workspaceID workspace.ID, sessionID string, cleanupJobID string) ([]byte, error) {
	return json.Marshal(map[string]string{
		"workspace_id":   string(workspaceID),
		"session_id":     sessionID,
		"cleanup_job_id": cleanupJobID,
	})
}
