package jobrunner

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/tetral-ai/tetral/internal/runtimecontrol"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/storage"
)

// Repair discovery cadence and bounds are fixed source constants. One run reads
// at most 8 pages of 128 raw bindings and performs at most 8 candidate
// mutations; with a 30 s cadence a lost Pod's bindings are reached within about
// ceil(bindings / 1024) runs.
const (
	runtimePodLossRepairInterval        = 30 * time.Second
	runtimePodLossRepairMinSpacing      = time.Second
	runtimePodLossRepairRunBudget       = 25 * time.Second
	runtimePodLossRepairPageTimeout     = time.Second
	runtimePodLossRepairMutationTimeout = 3 * time.Second
	runtimePodLossRepairPageSize        = 128
	runtimePodLossRepairPagesPerRun     = 8
	runtimePodLossRepairMutationsPerRun = 8
)

type runtimeBindingVisibilitySnapshotter interface {
	BindingVisibilitySnapshot() enginekubernetes.BindingVisibilitySnapshot
}

// RuntimePodLossRepairSignal coalesces Pod-deletion and process-takeover
// signals into one dirty bit for the repair owner.
type RuntimePodLossRepairSignal struct {
	dirty chan struct{}
}

func NewRuntimePodLossRepairSignal() *RuntimePodLossRepairSignal {
	return &RuntimePodLossRepairSignal{dirty: make(chan struct{}, 1)}
}

// Mark requests one repair run; repeated marks before it starts coalesce.
func (s *RuntimePodLossRepairSignal) Mark() {
	if s == nil {
		return
	}
	select {
	case s.dirty <- struct{}{}:
	default:
	}
}

type runtimeBindingKey struct {
	workspaceID string
	sessionID   string
}

type runtimePodLossCandidate struct {
	workspaceID       string
	sessionID         string
	bindingID         string
	bindingGeneration int64
	visibility        enginekubernetes.BindingVisibilityState
}

// RuntimePodLossRepair is the Runner's independent Pod-loss repair owner. It
// pages raw binding membership across Workspaces, nominates bindings whose
// process the existing predicate shows lost, and converges each through
// mutateLostRuntimeBinding, whose locked current-state checks stay
// authoritative. The cursor and pending candidates live only in this owner.
type RuntimePodLossRepair struct {
	store  *PostgreSQLRuntimeDeliveryStore
	signal *RuntimePodLossRepairSignal
	logger *slog.Logger

	mu       sync.Mutex
	inCycle  bool
	upper    runtimeBindingKey
	hasAfter bool
	cursor   runtimeBindingKey
	pending  []runtimePodLossCandidate

	// after, when set by in-package tests, replaces time.After for Run's
	// spacing and deadline timers so their order is observable without
	// wall-clock waits.
	after func(time.Duration) <-chan time.Time
}

// NewRuntimePodLossRepair builds the repair owner over the production delivery
// store. signal may be created first and shared with the Pod watcher and the
// notification listener.
func NewRuntimePodLossRepair(store *PostgreSQLRuntimeDeliveryStore, signal *RuntimePodLossRepairSignal, logger *slog.Logger) *RuntimePodLossRepair {
	if signal == nil {
		signal = NewRuntimePodLossRepairSignal()
	}
	return &RuntimePodLossRepair{store: store, signal: signal, logger: logger}
}

// Run starts one repair run at once, then one per 30 s deadline or coalesced
// signal, at least 1 s apart and never two at a time, until ctx ends. Both
// bounds count from the previous run's start.
func (r *RuntimePodLossRepair) Run(ctx context.Context) {
	if r == nil {
		return
	}
	after := r.after
	if after == nil {
		after = time.After
	}
	for {
		spacing, deadline := after(runtimePodLossRepairMinSpacing), after(runtimePodLossRepairInterval)
		r.RepairRun(ctx)
		select {
		case <-ctx.Done():
			return
		case <-spacing:
		}
		select {
		case <-ctx.Done():
			return
		case <-deadline:
		case <-r.signal.dirty:
		}
	}
}

// RuntimePodLossRepairRun reports one repair run: raw pages read, candidates
// nominated, and the outcome of each candidate mutation. Outcome is
// "completed", "failed" (a discovery query failed) or "snapshot_not_ready".
type RuntimePodLossRepairRun struct {
	Pages, Candidates, Repaired, Stale, Failed int
	Outcome                                    string
}

// Err reports a failed discovery query or failed candidate mutations; a stale
// candidate or an unready watcher is not an error.
func (run RuntimePodLossRepairRun) Err() error {
	if run.Outcome == "failed" || run.Failed > 0 {
		return fmt.Errorf("runtime pod-loss repair run %s with %d failed candidates", run.Outcome, run.Failed)
	}
	return nil
}

// RepairRun is the repair owner's entry point: one bounded run that drains
// pending candidates (at most 8 mutations), then reads further raw pages (at
// most 8) until the cycle reaches its upper bound, which ends the run. A page
// error or a not-ready watcher keeps the cursor for the next run.
func (r *RuntimePodLossRepair) RepairRun(ctx context.Context) RuntimePodLossRepairRun {
	r.mu.Lock()
	defer r.mu.Unlock()
	started := time.Now()
	runCtx, cancel := context.WithTimeout(ctx, runtimePodLossRepairRunBudget)
	defer cancel()
	summary := RuntimePodLossRepairRun{Outcome: "completed"}
	mutations := 0
	cycleEnded := false
	for {
		for len(r.pending) > 0 && mutations < runtimePodLossRepairMutationsPerRun && runCtx.Err() == nil {
			candidate := r.pending[0]
			r.pending = r.pending[1:]
			mutations++
			r.mutateCandidate(runCtx, candidate, &summary)
		}
		if len(r.pending) > 0 || cycleEnded || summary.Pages >= runtimePodLossRepairPagesPerRun || runCtx.Err() != nil {
			break
		}
		if !r.inCycle {
			upper, found, err := r.store.runtimeBindingUpper(runCtx)
			if err != nil {
				summary.Outcome = "failed"
				break
			}
			if !found {
				break
			}
			r.inCycle, r.upper, r.hasAfter = true, upper, false
		}
		page, ready, err := r.store.runtimeBindingPage(runCtx, r.hasAfter, r.cursor, r.upper)
		if err != nil {
			summary.Outcome = "failed"
			break
		}
		if !ready {
			summary.Outcome = "snapshot_not_ready"
			break
		}
		summary.Pages++
		summary.Candidates += len(page.candidates)
		for _, candidate := range page.candidates {
			r.logDetected(candidate)
		}
		// The worklist is owner state before the cursor moves past its page.
		r.pending = append(r.pending[:0], page.candidates...)
		if page.rows > 0 {
			r.hasAfter, r.cursor = true, page.last
		}
		if page.rows < runtimePodLossRepairPageSize || (r.hasAfter && r.cursor == r.upper) {
			r.inCycle, r.hasAfter = false, false
			cycleEnded = true
		}
	}
	r.logRun(summary, started)
	return summary
}

func (r *RuntimePodLossRepair) mutateCandidate(ctx context.Context, candidate runtimePodLossCandidate, summary *RuntimePodLossRepairRun) {
	mutationCtx, cancel := context.WithTimeout(ctx, runtimePodLossRepairMutationTimeout)
	defer cancel()
	started := time.Now()
	result, err := r.store.mutateLostRuntimeBinding(mutationCtx, candidate.workspaceID, candidate.sessionID, runtimecontrol.Binding{
		BindingID:         candidate.bindingID,
		BindingGeneration: candidate.bindingGeneration,
	}, r.store.runtimePodLossNow(), true)
	switch {
	case err != nil:
		summary.Failed++
		r.logRepairFailed(candidate)
	case result.status == runtimePodLossMutationStale:
		summary.Stale++
		r.logStale(candidate, result.staleReason)
	default:
		summary.Repaired++
		r.logRepaired(candidate, started)
	}
}

type runtimeBindingPage struct {
	rows       int
	last       runtimeBindingKey
	candidates []runtimePodLossCandidate
}

// runtimeBindingUpper captures a discovery cycle's upper bound by reverse
// binding primary-key seek.
func (s *PostgreSQLRuntimeDeliveryStore) runtimeBindingUpper(ctx context.Context) (runtimeBindingKey, bool, error) {
	if s == nil || s.Client == nil {
		return runtimeBindingKey{}, false, errors.New("runtime pod-loss repair store is unavailable")
	}
	queryCtx, cancel := context.WithTimeout(ctx, runtimePodLossRepairPageTimeout)
	defer cancel()
	var upper runtimeBindingKey
	err := s.Client.QueryRow(queryCtx, "jobrunner.runtime_binding_upper",
		`SELECT workspace_id, session_id FROM public.tetral_job_runner_binding_upper()`,
	).Scan(&upper.workspaceID, &upper.sessionID)
	if dbconnect.IsNoRows(err) {
		return runtimeBindingKey{}, false, nil
	}
	if err != nil {
		return runtimeBindingKey{}, false, err
	}
	return upper, true, nil
}

type runtimeBindingRawRow struct {
	key                runtimeBindingKey
	binding            runtimecontrol.Binding
	active, registered bool
	current            bool
	phase              string
}

// runtimeBindingPage reads one raw page in a short read-only repeatable-read
// transaction. Its query establishes the database snapshot before the single
// watcher snapshot for the page; the transaction closes before any mutation.
// ready is false when the watcher is not ready.
func (s *PostgreSQLRuntimeDeliveryStore) runtimeBindingPage(ctx context.Context, hasAfter bool, after runtimeBindingKey, upper runtimeBindingKey) (runtimeBindingPage, bool, error) {
	var page runtimeBindingPage
	if s == nil || s.Client == nil {
		return page, false, errors.New("runtime pod-loss repair store is unavailable")
	}
	snapshotter, ok := s.TargetResolver.(runtimeBindingVisibilitySnapshotter)
	if !ok || snapshotter == nil {
		return page, false, errors.New("runtime pod-loss visibility snapshot is unavailable")
	}
	var afterWorkspace, afterSession any
	if hasAfter {
		afterWorkspace, afterSession = after.workspaceID, after.sessionID
	}
	ready := false
	queryCtx, cancel := context.WithTimeout(ctx, runtimePodLossRepairPageTimeout)
	defer cancel()
	err := s.Client.WithTx(queryCtx, "jobrunner.runtime_binding_page", &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead}, func(tx *dbconnect.Tx) error {
		page, ready = runtimeBindingPage{}, false
		rows, err := tx.Query(queryCtx,
			`SELECT workspace_id, session_id, binding_id, binding_generation,
			        agent_runtime_namespace, agent_runtime_pod_name, agent_runtime_pod_uid, agent_runtime_pod_ip,
			        runtime_process_id, active, process_registered, is_current, phase
			   FROM public.tetral_job_runner_binding_page($1, $2, $3, $4, $5)`,
			afterWorkspace, afterSession, upper.workspaceID, upper.sessionID, runtimePodLossRepairPageSize,
		)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		var raw []runtimeBindingRawRow
		for rows.Next() {
			var row runtimeBindingRawRow
			if err := rows.Scan(
				&row.key.workspaceID, &row.key.sessionID, &row.binding.BindingID, &row.binding.BindingGeneration,
				&row.binding.Namespace, &row.binding.PodName, &row.binding.PodUID, &row.binding.PodIP,
				&row.binding.RuntimeProcessID, &row.active, &row.registered, &row.current, &row.phase,
			); err != nil {
				return err
			}
			raw = append(raw, row)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		snapshot := snapshotter.BindingVisibilitySnapshot()
		if !snapshot.Ready {
			return nil
		}
		ready = true
		page = nominateRuntimePodLossCandidates(raw, snapshot)
		return nil
	})
	if err != nil {
		return runtimeBindingPage{}, false, err
	}
	return page, ready, nil
}

// nominateRuntimePodLossCandidates applies the existing loss nomination to an
// active raw binding: a registered process past startup that is not current,
// not visibly reusable, or not accepting. Process absence or startup never
// nominates; the locked mutation re-decides every candidate.
func nominateRuntimePodLossCandidates(raw []runtimeBindingRawRow, snapshot enginekubernetes.BindingVisibilitySnapshot) runtimeBindingPage {
	page := runtimeBindingPage{rows: len(raw)}
	if len(raw) > 0 {
		page.last = raw[len(raw)-1].key
	}
	for _, row := range raw {
		if !row.active || !row.registered || row.phase == runtimecontrol.ProcessStarting {
			continue
		}
		visibility := snapshot.VisibilityFor(enginekubernetes.BoundRuntimePod{
			Namespace: row.binding.Namespace, PodName: row.binding.PodName,
			PodUID: row.binding.PodUID, PodIP: row.binding.PodIP,
		})
		if row.current && visibility == enginekubernetes.BindingVisibilityReusable && row.phase == runtimecontrol.ProcessAccepting {
			continue
		}
		page.candidates = append(page.candidates, runtimePodLossCandidate{
			workspaceID: row.key.workspaceID, sessionID: row.key.sessionID,
			bindingID: row.binding.BindingID, bindingGeneration: row.binding.BindingGeneration,
			visibility: visibility,
		})
	}
	return page
}

func (s *PostgreSQLRuntimeDeliveryStore) runtimePodLossNow() time.Time {
	if s.Clock != nil {
		return s.Clock().UTC()
	}
	return storage.Now()
}

func runtimePodLossIdentityAttrs(candidate runtimePodLossCandidate) []any {
	return []any{
		slog.String("workspace.id", candidate.workspaceID),
		slog.String("session.id", candidate.sessionID),
		slog.String("binding.id", candidate.bindingID),
		slog.Int64("binding.generation", candidate.bindingGeneration),
	}
}

func (r *RuntimePodLossRepair) logDetected(candidate runtimePodLossCandidate) {
	if r.logger == nil {
		return
	}
	attrs := append(runtimePodLossIdentityAttrs(candidate),
		slog.String("event", "runtime_pod_loss_detected"),
		slog.String("event.kind", "runtime_pod_loss_detected"),
		slog.String("component", ServiceNameJobRunner),
		slog.String("visibility.state", string(candidate.visibility)),
	)
	r.logger.Info("runtime_pod_loss_detected", attrs...)
}

func (r *RuntimePodLossRepair) logRepaired(candidate runtimePodLossCandidate, started time.Time) {
	if r.logger == nil {
		return
	}
	attrs := append(runtimePodLossIdentityAttrs(candidate),
		slog.String("event", "runtime_pod_loss_repaired"),
		slog.String("event.kind", "runtime_pod_loss_repaired"),
		slog.String("component", ServiceNameJobRunner),
		slog.String("outcome", "repaired"),
		slog.Int64("duration.ms", max(time.Since(started).Milliseconds(), 0)),
	)
	r.logger.Info("runtime_pod_loss_repaired", attrs...)
}

func (r *RuntimePodLossRepair) logStale(candidate runtimePodLossCandidate, reason string) {
	if r.logger == nil {
		return
	}
	attrs := append(runtimePodLossIdentityAttrs(candidate),
		slog.String("event", "runtime_pod_loss_stale"),
		slog.String("event.kind", "runtime_pod_loss_stale"),
		slog.String("component", ServiceNameJobRunner),
		slog.String("stale.reason", reason),
	)
	r.logger.Info("runtime_pod_loss_stale", attrs...)
}

func (r *RuntimePodLossRepair) logRepairFailed(candidate runtimePodLossCandidate) {
	if r.logger == nil {
		return
	}
	attrs := append(runtimePodLossIdentityAttrs(candidate),
		slog.String("event", "runtime_pod_loss_repair_failed"),
		slog.String("event.kind", "runtime_pod_loss_repair_failed"),
		slog.String("component", ServiceNameJobRunner),
		slog.String("stage", "mutation"),
		slog.String("error.class", "runtime_pod_loss_repair"),
		slog.String("error.code", "candidate_mutation_failed"),
		slog.String("error.message_safe", "runtime pod-loss candidate repair failed"),
	)
	r.logger.Error("runtime_pod_loss_repair_failed", attrs...)
}

func (r *RuntimePodLossRepair) logRun(summary RuntimePodLossRepairRun, started time.Time) {
	if r.logger == nil {
		return
	}
	attrs := []any{
		slog.String("event", "runtime_pod_loss_repair_run_completed"),
		slog.String("event.kind", "runtime_pod_loss_repair_run_completed"),
		slog.String("component", ServiceNameJobRunner),
		slog.Int("page.count", summary.Pages),
		slog.Int("candidate.count", summary.Candidates),
		slog.Int("repaired.count", summary.Repaired),
		slog.Int("stale.count", summary.Stale),
		slog.Int("failed.count", summary.Failed),
		slog.Int64("duration.ms", max(time.Since(started).Milliseconds(), 0)),
		slog.String("outcome", summary.Outcome),
	}
	if summary.Outcome == "failed" || summary.Failed > 0 {
		attrs = append(attrs,
			slog.String("error.class", "runtime_pod_loss_repair"),
			slog.String("error.code", "repair_run_failed"),
			slog.String("error.message_safe", "runtime pod-loss repair run completed with failures"),
		)
		r.logger.Error("runtime_pod_loss_repair_run_completed", attrs...)
		return
	}
	if summary.Outcome != "completed" || summary.Pages > 0 {
		r.logger.Info("runtime_pod_loss_repair_run_completed", attrs...)
	}
}
