package tetralsandbox

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const releaseIdempotencyRunningLease = 5 * time.Second

type ReleaseHandler struct {
	Client  *dbconnect.Client
	Service *sandbox.Service
	Store   sandbox.Store
}

func NewReleaseHandler(client *dbconnect.Client, service *sandbox.Service, store sandbox.Store) *ReleaseHandler {
	return &ReleaseHandler{Client: client, Service: service, Store: store}
}

// ReleaseSandbox is the sandbox-release RPC. A request carries exactly one
// identity class (validReleaseRequestIdentity): binding-scoped (BindingID +
// BindingGeneration > 0, no preparation/delete ids) for the archive-family
// reasons, or preparation-scoped (PreparationAttemptID + DeleteCleanupID, no
// binding ids) for delete. The idempotency row stores that identity class, and
// every replay is re-fenced against it (releaseIdempotencyFenceMatches); a
// request whose identity does not match the stored row is rejected.
//
//	reason            provider action (claim)   terminal sandbox status
//	cleanup           MarkReleasing             archived
//	runtime_pod_lost  MarkReleasing             archived
//	delete            MarkReleasingForDelete    released
//
// For delete the release candidate widens beyond the live set: when no live
// sandbox exists, the latest sandbox row is claimed as long as it still bears a
// provider handle (or the provider still resolves it by name), so a
// half-created machine is still torn down. The archive-family reasons only
// claim a live sandbox, or a failed one still needing cleanup.
//
// UPDATE-WITH: internal/sandbox (ClaimReleaseForSession,
// findReleaseCandidateForSession, completeSandboxReleaseState).
func (h *ReleaseHandler) ReleaseSandbox(ctx context.Context, request ReleaseSandboxRequest) (ReleaseSandboxResult, error) {
	if h == nil || h.Client == nil || h.Service == nil || h.Store == nil {
		return ReleaseSandboxResult{Status: ReleaseSandboxStatusRetryLater, SandboxStatus: "failed"}, nil
	}
	if !validReleaseRequestIdentity(request) || !validReleaseReason(request.Reason) || request.IdempotencyKey == "" {
		return ReleaseSandboxResult{Status: ReleaseSandboxStatusFailed, SandboxStatus: "failed"}, nil
	}
	ok, err := h.releaseFenceMatches(ctx, request)
	if err != nil {
		return ReleaseSandboxResult{Status: ReleaseSandboxStatusRetryLater, SandboxStatus: "failed"}, nil
	}
	if !ok {
		return ReleaseSandboxResult{Status: ReleaseSandboxStatusFailed, SandboxStatus: "failed"}, nil
	}
	replay, idempotencyClaimed, providerReleaseStarted, providerReleased, err := h.beginReleaseIdempotency(ctx, request)
	if err != nil {
		return ReleaseSandboxResult{Status: ReleaseSandboxStatusRetryLater, SandboxStatus: "failed"}, nil
	}
	if replay != nil {
		return *replay, nil
	}
	if !idempotencyClaimed {
		return ReleaseSandboxResult{Status: ReleaseSandboxStatusRetryLater, SandboxStatus: "releasing"}, nil
	}
	reason := sandbox.ReleaseReason(request.Reason)
	if providerReleased {
		result := h.completeProviderReleasedSandbox(ctx, request, reason)
		return h.finishProviderReleasedIdempotency(ctx, request, result), nil
	}
	if providerReleaseStarted {
		result := h.completeProviderReleaseStartedSandbox(ctx, request, reason)
		return h.finishProviderReleaseStartedIdempotency(ctx, request, result), nil
	}
	claimed, err := h.Service.ClaimReleaseForSession(ctx, workspace.ID(request.WorkspaceID), request.SessionID, reason)
	if err != nil {
		return h.finishReleaseIdempotency(ctx, request, ReleaseSandboxResult{Status: ReleaseSandboxStatusRetryLater, SandboxStatus: "failed"}), nil
	}
	if claimed == nil {
		return h.finishReleaseIdempotency(ctx, request, ReleaseSandboxResult{Status: ReleaseSandboxStatusAlreadyReleased, SandboxStatus: "released"}), nil
	}
	if claimed.ID != request.SandboxID {
		return h.finishReleaseIdempotency(ctx, request, ReleaseSandboxResult{Status: ReleaseSandboxStatusFailed, SandboxStatus: releaseResponseSandboxStatus(claimed.Status)}), nil
	}
	if err := h.markReleaseProviderStarted(ctx, request); err != nil {
		return h.finishReleaseIdempotency(ctx, request, ReleaseSandboxResult{Status: ReleaseSandboxStatusRetryLater, SandboxStatus: releaseResponseSandboxStatus(claimed.Status)}), nil
	}
	if err := h.Service.ReleaseClaimedSandboxProvider(ctx, claimed, reason); err != nil {
		return h.finishProviderReleaseStartedIdempotency(ctx, request, providerReleaseErrorResult(err, claimed.Status)), nil
	}
	if err := h.markReleaseProviderReleased(ctx, request); err != nil {
		return h.finishProviderReleaseStartedIdempotency(ctx, request, ReleaseSandboxResult{Status: ReleaseSandboxStatusRetryLater, SandboxStatus: releaseResponseSandboxStatus(claimed.Status)}), nil
	}
	if err := h.Service.CompleteProviderReleasedSandboxWithStore(ctx, h.Store, claimed, reason); err != nil {
		return ReleaseSandboxResult{Status: ReleaseSandboxStatusRetryLater, SandboxStatus: releaseResponseSandboxStatus(claimed.Status)}, nil
	}
	return h.finishReleaseIdempotency(ctx, request, successfulReleaseResult(reason)), nil
}

func (h *ReleaseHandler) beginReleaseIdempotency(ctx context.Context, request ReleaseSandboxRequest) (*ReleaseSandboxResult, bool, bool, bool, error) {
	now := storage.Now()
	var replay *ReleaseSandboxResult
	claimed := false
	providerReleaseStarted := false
	providerReleased := false
	err := h.Client.WithWorkspaceTx(ctx, request.WorkspaceID, "tetralsandbox.release_idempotency_begin", func(tx *dbconnect.Tx) error {
		row, err := h.loadReleaseIdempotencyByKey(ctx, tx, request.WorkspaceID, request.IdempotencyKey)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		if err == nil {
			if !releaseIdempotencyFenceMatches(row, request) {
				replay = &ReleaseSandboxResult{Status: ReleaseSandboxStatusFailed, SandboxStatus: "failed"}
				return nil
			}
			if row.Status == "completed" {
				result := releaseResultFromStored(row.ResponseStatus, row.SandboxStatus)
				replay = &result
				return nil
			}
			if row.Status == "provider_released" {
				claimed = true
				providerReleased = true
				return nil
			}
			if row.Status == "provider_release_started" {
				claimed = true
				providerReleaseStarted = true
				return nil
			}
			if runningFresh(now, row.UpdatedAt) {
				replay = &ReleaseSandboxResult{Status: ReleaseSandboxStatusRetryLater, SandboxStatus: storedSandboxStatus(row.SandboxStatus, "releasing")}
				return nil
			}
			if _, err := tx.Exec(ctx,
				`UPDATE sandbox_release_idempotency_keys
				    SET updated_at = $3,
				        response_status = NULL,
				        sandbox_status = NULL
				  WHERE workspace_id = $1
				    AND idempotency_key = $2`,
				request.WorkspaceID,
				request.IdempotencyKey,
				formatReleaseIDTime(now),
			); err != nil {
				return err
			}
			claimed = true
			return nil
		}

		row, err = h.loadReleaseIdempotencyByFence(ctx, tx, request)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		if err == nil {
			if !releaseIdempotencyFenceMatches(row, request) {
				replay = &ReleaseSandboxResult{Status: ReleaseSandboxStatusFailed, SandboxStatus: "failed"}
				return nil
			}
			replay = &ReleaseSandboxResult{Status: ReleaseSandboxStatusFailed, SandboxStatus: "failed"}
			return nil
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO sandbox_release_idempotency_keys (
				workspace_id, idempotency_key, session_id, sandbox_id, binding_id,
				binding_generation, preparation_attempt_id, delete_cleanup_id, reason,
				status, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'running', $10, $10)`,
			request.WorkspaceID,
			request.IdempotencyKey,
			request.SessionID,
			request.SandboxID,
			nullableReleaseString(request.BindingID),
			nullableReleaseGeneration(request.BindingGeneration),
			nullableReleaseString(request.PreparationAttemptID),
			nullableReleaseString(request.DeleteCleanupID),
			request.Reason,
			formatReleaseIDTime(now),
		); err != nil {
			return err
		}
		claimed = true
		return nil
	})
	return replay, claimed, providerReleaseStarted, providerReleased, err
}

func (h *ReleaseHandler) markReleaseProviderStarted(ctx context.Context, request ReleaseSandboxRequest) error {
	now := formatReleaseIDTime(storage.Now())
	return h.Client.WithWorkspaceTx(ctx, request.WorkspaceID, "tetralsandbox.release_idempotency_provider_release_started", func(tx *dbconnect.Tx) error {
		result, err := tx.Exec(ctx,
			`UPDATE sandbox_release_idempotency_keys
			    SET status = 'provider_release_started',
			        response_status = NULL,
			        sandbox_status = 'releasing',
			        updated_at = $3
			  WHERE workspace_id = $1
			    AND idempotency_key = $2
			    AND session_id = $4
			    AND sandbox_id = $5
			    AND status = 'running'`,
			request.WorkspaceID,
			request.IdempotencyKey,
			now,
			request.SessionID,
			request.SandboxID,
		)
		if err != nil {
			return err
		}
		if affected, _ := result.RowsAffected(); affected != 1 {
			return sql.ErrNoRows
		}
		return nil
	})
}

func (h *ReleaseHandler) markReleaseProviderReleased(ctx context.Context, request ReleaseSandboxRequest) error {
	now := formatReleaseIDTime(storage.Now())
	return h.Client.WithWorkspaceTx(ctx, request.WorkspaceID, "tetralsandbox.release_idempotency_provider_released", func(tx *dbconnect.Tx) error {
		result, err := tx.Exec(ctx,
			`UPDATE sandbox_release_idempotency_keys
			    SET status = 'provider_released',
			        response_status = NULL,
			        sandbox_status = 'releasing',
			        updated_at = $3
			  WHERE workspace_id = $1
			    AND idempotency_key = $2
				    AND session_id = $4
				    AND sandbox_id = $5
				    AND status IN ('provider_release_started', 'provider_released')`,
			request.WorkspaceID,
			request.IdempotencyKey,
			now,
			request.SessionID,
			request.SandboxID,
		)
		if err != nil {
			return err
		}
		if affected, _ := result.RowsAffected(); affected != 1 {
			return sql.ErrNoRows
		}
		return nil
	})
}

func (h *ReleaseHandler) completeProviderReleaseStartedSandbox(ctx context.Context, request ReleaseSandboxRequest, reason sandbox.ReleaseReason) ReleaseSandboxResult {
	current, err := h.Store.FindLatestBySessionID(ctx, workspace.ID(request.WorkspaceID), request.SessionID)
	if err != nil {
		return ReleaseSandboxResult{Status: ReleaseSandboxStatusRetryLater, SandboxStatus: "failed"}
	}
	if current == nil {
		return ReleaseSandboxResult{Status: ReleaseSandboxStatusAlreadyReleased, SandboxStatus: "released"}
	}
	if current.ID != request.SandboxID {
		return ReleaseSandboxResult{Status: ReleaseSandboxStatusFailed, SandboxStatus: releaseResponseSandboxStatus(current.Status)}
	}
	if current.Status == terminalSandboxState(reason) {
		return successfulReleaseResult(reason)
	}
	if current.Status != sandbox.StatusReleasing {
		return ReleaseSandboxResult{Status: ReleaseSandboxStatusFailed, SandboxStatus: releaseResponseSandboxStatus(current.Status)}
	}
	status, err := h.Service.ObserveClaimedRelease(ctx, current, reason)
	if err != nil {
		return providerReleaseErrorResult(err, current.Status)
	}
	providerTerminal := status.Availability == sandbox.ProviderMissing
	if reason != sandbox.ReleaseReasonDelete && status.SandboxStatus == sandbox.StatusArchived {
		providerTerminal = true
	}
	if !providerTerminal {
		return ReleaseSandboxResult{Status: ReleaseSandboxStatusRetryLater, SandboxStatus: releaseResponseSandboxStatus(current.Status)}
	}
	if err := h.markReleaseProviderReleased(ctx, request); err != nil {
		return ReleaseSandboxResult{Status: ReleaseSandboxStatusRetryLater, SandboxStatus: releaseResponseSandboxStatus(current.Status)}
	}
	return h.completeProviderReleasedSandbox(ctx, request, reason)
}

func providerReleaseErrorResult(err error, status sandbox.Status) ReleaseSandboxResult {
	result := ReleaseSandboxResult{Status: ReleaseSandboxStatusRetryLater, SandboxStatus: releaseResponseSandboxStatus(status)}
	var providerErr *sandbox.ProviderError
	if errors.As(err, &providerErr) && !providerErr.Retryable {
		result.Status = ReleaseSandboxStatusFailed
	}
	return result
}

func (h *ReleaseHandler) completeProviderReleasedSandbox(ctx context.Context, request ReleaseSandboxRequest, reason sandbox.ReleaseReason) ReleaseSandboxResult {
	current, err := h.Store.FindLatestBySessionID(ctx, workspace.ID(request.WorkspaceID), request.SessionID)
	if err != nil {
		return ReleaseSandboxResult{Status: ReleaseSandboxStatusRetryLater, SandboxStatus: "failed"}
	}
	if current == nil {
		return ReleaseSandboxResult{Status: ReleaseSandboxStatusAlreadyReleased, SandboxStatus: "released"}
	}
	if current.ID != request.SandboxID {
		return ReleaseSandboxResult{Status: ReleaseSandboxStatusFailed, SandboxStatus: releaseResponseSandboxStatus(current.Status)}
	}
	if current.Status == terminalSandboxState(reason) {
		return successfulReleaseResult(reason)
	}
	if current.Status != sandbox.StatusReleasing {
		return ReleaseSandboxResult{Status: ReleaseSandboxStatusFailed, SandboxStatus: releaseResponseSandboxStatus(current.Status)}
	}
	if err := h.Service.CompleteProviderReleasedSandboxWithStore(ctx, h.Store, current, reason); err != nil {
		return ReleaseSandboxResult{Status: ReleaseSandboxStatusRetryLater, SandboxStatus: releaseResponseSandboxStatus(current.Status)}
	}
	return successfulReleaseResult(reason)
}

func (h *ReleaseHandler) finishReleaseIdempotency(ctx context.Context, request ReleaseSandboxRequest, result ReleaseSandboxResult) ReleaseSandboxResult {
	status := "completed"
	if result.Status == ReleaseSandboxStatusRetryLater {
		status = "running"
	}
	return h.storeReleaseIdempotencyResult(ctx, request, status, result)
}

func (h *ReleaseHandler) finishProviderReleasedIdempotency(ctx context.Context, request ReleaseSandboxRequest, result ReleaseSandboxResult) ReleaseSandboxResult {
	status := "completed"
	if result.Status == ReleaseSandboxStatusRetryLater {
		status = "provider_released"
	}
	return h.storeReleaseIdempotencyResult(ctx, request, status, result)
}

func (h *ReleaseHandler) finishProviderReleaseStartedIdempotency(ctx context.Context, request ReleaseSandboxRequest, result ReleaseSandboxResult) ReleaseSandboxResult {
	status := "completed"
	if result.Status == ReleaseSandboxStatusRetryLater {
		status = "provider_release_started"
	}
	return h.storeReleaseIdempotencyResult(ctx, request, status, result)
}

func (h *ReleaseHandler) storeReleaseIdempotencyResult(ctx context.Context, request ReleaseSandboxRequest, status string, result ReleaseSandboxResult) ReleaseSandboxResult {
	result.SandboxStatus = normalizeReleaseSandboxStatus(result.SandboxStatus)
	now := formatReleaseIDTime(storage.Now())
	err := h.Client.WithWorkspaceTx(ctx, request.WorkspaceID, "tetralsandbox.release_idempotency_finish", func(tx *dbconnect.Tx) error {
		update, err := tx.Exec(ctx,
			`UPDATE sandbox_release_idempotency_keys
			    SET status = $3,
			        response_status = $4,
			        sandbox_status = $5,
			        updated_at = $6
			  WHERE workspace_id = $1
			    AND idempotency_key = $2
			    AND session_id = $7
			    AND sandbox_id = $8`,
			request.WorkspaceID,
			request.IdempotencyKey,
			status,
			string(result.Status),
			result.SandboxStatus,
			now,
			request.SessionID,
			request.SandboxID,
		)
		if err != nil {
			return err
		}
		if affected, _ := update.RowsAffected(); affected != 1 {
			return sql.ErrNoRows
		}
		return nil
	})
	if err != nil {
		return ReleaseSandboxResult{Status: ReleaseSandboxStatusRetryLater, SandboxStatus: "failed"}
	}
	return result
}

type releaseIdempotencyRow struct {
	IdempotencyKey       string
	SessionID            string
	SandboxID            string
	BindingID            string
	BindingGeneration    int64
	PreparationAttemptID string
	DeleteCleanupID      string
	Reason               string
	Status               string
	ResponseStatus       sql.NullString
	SandboxStatus        sql.NullString
	UpdatedAt            string
}

func (h *ReleaseHandler) loadReleaseIdempotencyByKey(ctx context.Context, tx *dbconnect.Tx, workspaceID string, idempotencyKey string) (releaseIdempotencyRow, error) {
	var row releaseIdempotencyRow
	err := tx.QueryRow(ctx,
		`SELECT idempotency_key, session_id, sandbox_id, COALESCE(binding_id, ''), COALESCE(binding_generation, 0),
		        COALESCE(preparation_attempt_id, ''), COALESCE(delete_cleanup_id, ''), reason,
		        status, response_status, sandbox_status, updated_at
		   FROM sandbox_release_idempotency_keys
		  WHERE workspace_id = $1
		    AND idempotency_key = $2
		  FOR UPDATE`,
		workspaceID,
		idempotencyKey,
	).Scan(
		&row.IdempotencyKey,
		&row.SessionID,
		&row.SandboxID,
		&row.BindingID,
		&row.BindingGeneration,
		&row.PreparationAttemptID,
		&row.DeleteCleanupID,
		&row.Reason,
		&row.Status,
		&row.ResponseStatus,
		&row.SandboxStatus,
		&row.UpdatedAt,
	)
	return row, err
}

func (h *ReleaseHandler) loadReleaseIdempotencyByFence(ctx context.Context, tx *dbconnect.Tx, request ReleaseSandboxRequest) (releaseIdempotencyRow, error) {
	var row releaseIdempotencyRow
	err := tx.QueryRow(ctx,
		`SELECT idempotency_key, session_id, sandbox_id, COALESCE(binding_id, ''), COALESCE(binding_generation, 0),
		        COALESCE(preparation_attempt_id, ''), COALESCE(delete_cleanup_id, ''), reason,
		        status, response_status, sandbox_status, updated_at
		   FROM sandbox_release_idempotency_keys
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND sandbox_id = $3
		    AND (
		      (binding_id = $4 AND binding_generation = $5 AND preparation_attempt_id IS NULL AND delete_cleanup_id IS NULL)
		      OR (binding_id IS NULL AND binding_generation IS NULL AND preparation_attempt_id = $6 AND delete_cleanup_id = $7)
		    )
		  FOR UPDATE`,
		request.WorkspaceID,
		request.SessionID,
		request.SandboxID,
		request.BindingID,
		request.BindingGeneration,
		request.PreparationAttemptID,
		request.DeleteCleanupID,
	).Scan(
		&row.IdempotencyKey,
		&row.SessionID,
		&row.SandboxID,
		&row.BindingID,
		&row.BindingGeneration,
		&row.PreparationAttemptID,
		&row.DeleteCleanupID,
		&row.Reason,
		&row.Status,
		&row.ResponseStatus,
		&row.SandboxStatus,
		&row.UpdatedAt,
	)
	return row, err
}

func releaseIdempotencyFenceMatches(row releaseIdempotencyRow, request ReleaseSandboxRequest) bool {
	return row.SessionID == request.SessionID &&
		row.SandboxID == request.SandboxID &&
		row.BindingID == request.BindingID &&
		row.BindingGeneration == request.BindingGeneration &&
		row.PreparationAttemptID == request.PreparationAttemptID &&
		row.DeleteCleanupID == request.DeleteCleanupID &&
		row.Reason == request.Reason
}

func releaseResultFromStored(status sql.NullString, sandboxStatus sql.NullString) ReleaseSandboxResult {
	return ReleaseSandboxResult{
		Status:        releaseStatusFromString(storedSandboxStatus(status, string(ReleaseSandboxStatusFailed))),
		SandboxStatus: normalizeReleaseSandboxStatus(storedSandboxStatus(sandboxStatus, "failed")),
	}
}

func releaseResponseSandboxStatus(status sandbox.Status) string {
	return normalizeReleaseSandboxStatus(string(status))
}

func normalizeReleaseSandboxStatus(status string) string {
	switch status {
	case "releasing", "released", "archived", "failed":
		return status
	default:
		return "failed"
	}
}

func releaseStatusFromString(status string) ReleaseSandboxStatus {
	switch ReleaseSandboxStatus(status) {
	case ReleaseSandboxStatusReleased, ReleaseSandboxStatusArchived, ReleaseSandboxStatusAlreadyReleased, ReleaseSandboxStatusRetryLater, ReleaseSandboxStatusFailed:
		return ReleaseSandboxStatus(status)
	default:
		return ReleaseSandboxStatusFailed
	}
}

func storedSandboxStatus(value sql.NullString, fallback string) string {
	if value.Valid && value.String != "" {
		return value.String
	}
	return fallback
}

func runningFresh(now time.Time, updatedAt string) bool {
	parsed, err := time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		parsed, err = time.Parse(time.RFC3339, updatedAt)
	}
	if err != nil {
		return false
	}
	return now.Sub(parsed) < releaseIdempotencyRunningLease
}

func formatReleaseIDTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func validReleaseReason(reason string) bool {
	switch sandbox.ReleaseReason(reason) {
	case sandbox.ReleaseReasonCleanup, sandbox.ReleaseReasonRuntimePodLost, sandbox.ReleaseReasonDelete:
		return true
	default:
		return false
	}
}

func validReleaseRequestIdentity(request ReleaseSandboxRequest) bool {
	binding := request.BindingID != "" && request.BindingGeneration > 0 && request.PreparationAttemptID == "" && request.DeleteCleanupID == ""
	preparation := request.BindingID == "" && request.BindingGeneration == 0 && request.PreparationAttemptID != "" && request.DeleteCleanupID != ""
	if request.Reason == string(sandbox.ReleaseReasonDelete) {
		return preparation
	}
	return binding
}

func nullableReleaseString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableReleaseGeneration(value int64) any {
	if value == 0 {
		return nil
	}
	return value
}

func terminalSandboxState(reason sandbox.ReleaseReason) sandbox.Status {
	if reason == sandbox.ReleaseReasonDelete {
		return sandbox.StatusReleased
	}
	return sandbox.StatusArchived
}

func successfulReleaseResult(reason sandbox.ReleaseReason) ReleaseSandboxResult {
	if reason == sandbox.ReleaseReasonDelete {
		return ReleaseSandboxResult{Status: ReleaseSandboxStatusReleased, SandboxStatus: "released"}
	}
	return ReleaseSandboxResult{Status: ReleaseSandboxStatusArchived, SandboxStatus: "archived"}
}

func (h *ReleaseHandler) releaseFenceMatches(ctx context.Context, request ReleaseSandboxRequest) (bool, error) {
	var exists bool
	err := h.Client.WithWorkspaceTx(ctx, request.WorkspaceID, "tetralsandbox.release_fence", func(tx *dbconnect.Tx) error {
		if request.Reason == string(sandbox.ReleaseReasonDelete) {
			return tx.QueryRow(ctx,
				`SELECT EXISTS (
				SELECT 1
				  FROM sessions s
				  JOIN session_preparations sp
				    ON sp.workspace_id = s.workspace_id
				   AND sp.session_id = s.id
				   AND sp.preparation_attempt_id = $4
				   AND sp.sandbox_id = $3
				   AND sp.superseded_at IS NULL
				 WHERE s.workspace_id = $1
				   AND s.id = $2
				   AND s.lifecycle_state = 'deleted'
				   AND s.delete_cleanup_id = $5
				   AND NOT EXISTS (
				     SELECT 1 FROM session_runtime_bindings rb
				      WHERE rb.workspace_id = s.workspace_id AND rb.session_id = s.id
				   )
				)`,
				request.WorkspaceID,
				request.SessionID,
				request.SandboxID,
				request.PreparationAttemptID,
				request.DeleteCleanupID,
			).Scan(&exists)
		}
		cleanupRequired := request.Reason == string(sandbox.ReleaseReasonCleanup)
		return tx.QueryRow(ctx,
			`SELECT EXISTS (
			SELECT 1
			  FROM session_runtime_status rs
				  JOIN session_preparations sp
				    ON sp.workspace_id = rs.workspace_id
				   AND sp.session_id = rs.session_id
				   AND sp.sandbox_id = $3
				   AND sp.superseded_at IS NULL
				 WHERE rs.workspace_id = $1
			   AND rs.session_id = $2
			   AND rs.binding_id = $4
			   AND rs.binding_generation = $5
			   AND (NOT $6 OR (rs.cleanup_job_id IS NOT NULL AND rs.cleanup_claimed_at IS NOT NULL))
		)`,
			request.WorkspaceID,
			request.SessionID,
			request.SandboxID,
			request.BindingID,
			request.BindingGeneration,
			cleanupRequired,
		).Scan(&exists)
	})
	return exists, err
}
