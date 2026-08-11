package agentruntimebridge

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/storage"
)

const runtimePodLossCensusPageSize = 32

type runtimeBindingVisibilitySnapshotter interface {
	BindingVisibilitySnapshot() enginekubernetes.BindingVisibilitySnapshot
}

type runtimePodLossCandidate struct {
	sessionID         string
	bindingID         string
	bindingGeneration int64
	visibility        enginekubernetes.BindingVisibilityState
}

type runtimePodLossCensus struct {
	snapshotReady bool
	pageCount     int
	candidates    []runtimePodLossCandidate
}

type runtimePodLossCandidateRepairError struct {
	sessionID string
	cause     error
}

func (e runtimePodLossCandidateRepairError) Error() string {
	return "runtime pod-loss repair failed for session " + e.sessionID
}

func (e runtimePodLossCandidateRepairError) Unwrap() error { return e.cause }

// RepairLostRuntimeBindings freezes the active binding membership for one workspace,
// closes the read snapshot, and then converges each proven-gone identity through the
// same Session-locked mutation used by input-triggered recovery.
func (s *PostgreSQLRuntimeDeliveryStore) RepairLostRuntimeBindings(ctx context.Context, workspaceID string) (int, error) {
	started := time.Now()
	census, err := s.runtimePodLossCensus(ctx, workspaceID)
	if err != nil {
		s.logRuntimePodLossSweep(workspaceID, census, 0, 0, 0, started, "failed", true)
		return 0, err
	}
	if !census.snapshotReady {
		s.logRuntimePodLossSweep(workspaceID, census, 0, 0, 0, started, "snapshot_not_ready", false)
		return 0, nil
	}
	for _, candidate := range census.candidates {
		s.logRuntimePodLossDetected(workspaceID, candidate)
	}

	repaired := 0
	stale := 0
	failed := 0
	var candidateErrs []error
	for _, candidate := range census.candidates {
		if ctx.Err() != nil {
			candidateErrs = append(candidateErrs, ctx.Err())
			break
		}
		candidateStarted := time.Now()
		result, mutationErr := s.mutateLostRuntimeBinding(
			ctx,
			workspaceID,
			candidate.sessionID,
			runtimeBindingForDelivery{
				BindingID:         candidate.bindingID,
				BindingGeneration: candidate.bindingGeneration,
			},
			s.runtimePodLossNow(),
			true,
		)
		if mutationErr != nil {
			failed++
			s.logRuntimePodLossRepairFailed(workspaceID, candidate, "mutation")
			candidateErrs = append(candidateErrs, runtimePodLossCandidateRepairError{
				sessionID: candidate.sessionID,
				cause:     mutationErr,
			})
			continue
		}
		if result.status == runtimePodLossMutationStale {
			stale++
			s.logRuntimePodLossStale(workspaceID, candidate, result.staleReason)
			continue
		}
		repaired++
		s.logRuntimePodLossRepaired(workspaceID, candidate, candidateStarted)
	}
	joined := errors.Join(candidateErrs...)
	outcome := "completed"
	if joined != nil {
		outcome = "failed"
	}
	s.logRuntimePodLossSweep(
		workspaceID,
		census,
		repaired,
		stale,
		failed,
		started,
		outcome,
		joined != nil,
	)
	return repaired, joined
}

// runtimePodLossCensus holds one repeatable-read transaction only while paging
// durable identities. The first page query establishes the database snapshot before
// the watcher snapshot is read; mutations begin only after this function returns.
func (s *PostgreSQLRuntimeDeliveryStore) runtimePodLossCensus(ctx context.Context, workspaceID string) (runtimePodLossCensus, error) {
	var census runtimePodLossCensus
	if s == nil || s.Client == nil {
		return census, errors.New("runtime pod-loss repair store is unavailable")
	}
	snapshotter, ok := s.TargetResolver.(runtimeBindingVisibilitySnapshotter)
	if !ok || snapshotter == nil {
		return census, errors.New("runtime pod-loss visibility snapshot is unavailable")
	}
	err := s.Client.WithWorkspaceReadOnlyRepeatableReadTx(
		ctx,
		workspaceID,
		"agentruntimebridge.runtime_pod_loss_census",
		func(tx *dbconnect.Tx) error {
			cursorSessionID := ""
			var cursorGeneration int64
			cursorBindingID := ""
			firstPage := true
			var snapshot enginekubernetes.BindingVisibilitySnapshot
			for {
				page, err := runtimePodLossCensusPageTx(ctx, tx, workspaceID, cursorSessionID, cursorGeneration, cursorBindingID)
				if err != nil {
					return err
				}
				if firstPage {
					firstPage = false
					snapshot = snapshotter.BindingVisibilitySnapshot()
					census.snapshotReady = snapshot.Ready
					if !snapshot.Ready {
						return nil
					}
				}
				if len(page) == 0 {
					return nil
				}
				census.pageCount++
				for _, row := range page {
					visibility := snapshot.VisibilityFor(enginekubernetes.BoundRuntimePod{
						Namespace: row.binding.Namespace,
						PodName:   row.binding.PodName,
						PodUID:    row.binding.PodUID,
						PodIP:     row.binding.PodIP,
					})
					if classifyRuntimeBindingVisibility(visibility) == runtimeBindingVisibilityProvenGone {
						census.candidates = append(census.candidates, runtimePodLossCandidate{
							sessionID:         row.sessionID,
							bindingID:         row.binding.BindingID,
							bindingGeneration: row.binding.BindingGeneration,
							visibility:        visibility,
						})
					}
				}
				last := page[len(page)-1]
				cursorSessionID = last.sessionID
				cursorGeneration = last.binding.BindingGeneration
				cursorBindingID = last.binding.BindingID
				if len(page) < runtimePodLossCensusPageSize {
					return nil
				}
			}
		},
	)
	return census, err
}

type runtimePodLossCensusRow struct {
	sessionID string
	binding   runtimeBindingForDelivery
}

func runtimePodLossCensusPageTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	cursorSessionID string,
	cursorGeneration int64,
	cursorBindingID string,
) ([]runtimePodLossCensusRow, error) {
	rows, err := tx.Query(ctx,
		`SELECT binding.session_id,
		        binding.binding_id,
		        binding.binding_generation,
		        binding.agent_runtime_namespace,
		        binding.agent_runtime_pod_name,
		        binding.agent_runtime_pod_uid,
		        binding.agent_runtime_pod_ip
		   FROM session_runtime_bindings binding
		   JOIN session_runtime_status runtime
		     ON runtime.workspace_id = binding.workspace_id
		    AND runtime.session_id = binding.session_id
		    AND runtime.binding_id = binding.binding_id
		    AND runtime.binding_generation = binding.binding_generation
		   JOIN sessions session
		     ON session.workspace_id = binding.workspace_id
		    AND session.id = binding.session_id
		  WHERE binding.workspace_id = $1
		    AND (
		      runtime.status = 'running'
		      OR session.status = 'rescheduling'
		      OR EXISTS (
		        SELECT 1
		          FROM session_runtime_inbox inbox
		         WHERE inbox.workspace_id = binding.workspace_id
		           AND inbox.session_id = binding.session_id
		           AND inbox.status = 'accepted'
		           AND inbox.binding_id = binding.binding_id
		           AND inbox.binding_generation = binding.binding_generation
		           AND inbox.target_pod_uid = binding.agent_runtime_pod_uid
		      )
		    )
		    AND ($2 = '' OR (binding.session_id, binding.binding_generation, binding.binding_id) > ($2, $3, $4))
		  ORDER BY binding.session_id, binding.binding_generation, binding.binding_id
		  LIMIT $5`,
		workspaceID,
		cursorSessionID,
		cursorGeneration,
		cursorBindingID,
		runtimePodLossCensusPageSize,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	page := make([]runtimePodLossCensusRow, 0, runtimePodLossCensusPageSize)
	for rows.Next() {
		var row runtimePodLossCensusRow
		if err := rows.Scan(
			&row.sessionID,
			&row.binding.BindingID,
			&row.binding.BindingGeneration,
			&row.binding.Namespace,
			&row.binding.PodName,
			&row.binding.PodUID,
			&row.binding.PodIP,
		); err != nil {
			return nil, err
		}
		page = append(page, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return page, nil
}

func (s *PostgreSQLRuntimeDeliveryStore) runtimePodLossNow() time.Time {
	if s.Clock != nil {
		return s.Clock().UTC()
	}
	return storage.Now()
}

func runtimePodLossIdentityAttrs(workspaceID string, candidate runtimePodLossCandidate) []any {
	return []any{
		slog.String("workspace.id", workspaceID),
		slog.String("session.id", candidate.sessionID),
		slog.String("binding.id", candidate.bindingID),
		slog.Int64("binding.generation", candidate.bindingGeneration),
	}
}

func (s *PostgreSQLRuntimeDeliveryStore) logRuntimePodLossDetected(workspaceID string, candidate runtimePodLossCandidate) {
	if s.Logger == nil {
		return
	}
	attrs := append(runtimePodLossIdentityAttrs(workspaceID, candidate),
		slog.String("event", "runtime_pod_loss_detected"),
		slog.String("event.kind", "runtime_pod_loss_detected"),
		slog.String("component", ServiceNameJobRunner),
		slog.String("visibility.state", string(candidate.visibility)),
	)
	s.Logger.Info("runtime_pod_loss_detected", attrs...)
}

func (s *PostgreSQLRuntimeDeliveryStore) logRuntimePodLossRepaired(workspaceID string, candidate runtimePodLossCandidate, started time.Time) {
	if s.Logger == nil {
		return
	}
	attrs := append(runtimePodLossIdentityAttrs(workspaceID, candidate),
		slog.String("event", "runtime_pod_loss_repaired"),
		slog.String("event.kind", "runtime_pod_loss_repaired"),
		slog.String("component", ServiceNameJobRunner),
		slog.String("outcome", "repaired"),
		slog.Int64("duration.ms", max(time.Since(started).Milliseconds(), 0)),
	)
	s.Logger.Info("runtime_pod_loss_repaired", attrs...)
}

func (s *PostgreSQLRuntimeDeliveryStore) logRuntimePodLossStale(workspaceID string, candidate runtimePodLossCandidate, reason string) {
	if s.Logger == nil {
		return
	}
	attrs := append(runtimePodLossIdentityAttrs(workspaceID, candidate),
		slog.String("event", "runtime_pod_loss_stale"),
		slog.String("event.kind", "runtime_pod_loss_stale"),
		slog.String("component", ServiceNameJobRunner),
		slog.String("stale.reason", reason),
	)
	s.Logger.Info("runtime_pod_loss_stale", attrs...)
}

func (s *PostgreSQLRuntimeDeliveryStore) logRuntimePodLossRepairFailed(workspaceID string, candidate runtimePodLossCandidate, stage string) {
	if s.Logger == nil {
		return
	}
	attrs := append(runtimePodLossIdentityAttrs(workspaceID, candidate),
		slog.String("event", "runtime_pod_loss_repair_failed"),
		slog.String("event.kind", "runtime_pod_loss_repair_failed"),
		slog.String("component", ServiceNameJobRunner),
		slog.String("stage", stage),
		slog.String("error.class", "runtime_pod_loss_repair"),
		slog.String("error.code", "candidate_mutation_failed"),
		slog.String("error.message_safe", "runtime pod-loss candidate repair failed"),
	)
	s.Logger.Error("runtime_pod_loss_repair_failed", attrs...)
}

func (s *PostgreSQLRuntimeDeliveryStore) logRuntimePodLossSweep(
	workspaceID string,
	census runtimePodLossCensus,
	repaired int,
	stale int,
	failed int,
	started time.Time,
	outcome string,
	isError bool,
) {
	if s.Logger == nil {
		return
	}
	attrs := []any{
		slog.String("event", "runtime_pod_loss_sweep_completed"),
		slog.String("event.kind", "runtime_pod_loss_sweep_completed"),
		slog.String("component", ServiceNameJobRunner),
		slog.String("workspace.id", workspaceID),
		slog.Int("page.count", census.pageCount),
		slog.Int("candidate.count", len(census.candidates)),
		slog.Int("repaired.count", repaired),
		slog.Int("stale.count", stale),
		slog.Int("failed.count", failed),
		slog.Int64("duration.ms", max(time.Since(started).Milliseconds(), 0)),
		slog.String("outcome", outcome),
	}
	if isError {
		attrs = append(attrs,
			slog.String("error.class", "runtime_pod_loss_sweep"),
			slog.String("error.code", "sweep_failed"),
			slog.String("error.message_safe", "runtime pod-loss sweep completed with failures"),
		)
		s.Logger.Error("runtime_pod_loss_sweep_completed", attrs...)
		return
	}
	s.Logger.Info("runtime_pod_loss_sweep_completed", attrs...)
}
