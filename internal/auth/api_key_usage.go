package auth

import (
	"cmp"
	"context"
	"crypto/sha256"
	"database/sql"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// API-key usage is telemetry: a bounded, lossy sample of committed admissions,
// never a durable queue, security evidence or a last-request guarantee.
const (
	apiKeyUsageCapacity            = 4096
	apiKeyUsageTickInterval        = time.Second
	apiKeyUsageBatchSize           = 128
	apiKeyUsageSuppression         = 5 * time.Minute
	apiKeyUsageBatchDeadline       = time.Second
	apiKeyUsageTransactionDeadline = 100 * time.Millisecond
)

// apiKeyUsageUpdate writes a sample only to the credential material, generation
// and workspace it was admitted under, and only when its database observation
// is at least five minutes after the stored sample. last_used_at never moves
// backward. A zero-row result is a normal coalesced, stale or revoked sample.
const apiKeyUsageUpdate = `UPDATE public.api_keys
SET last_used_at = $5
WHERE workspace_id = $1 AND id = $2
  AND key_digest = $3 AND usage_generation = $4
  AND revoked_at IS NULL
  AND (last_used_at IS NULL OR $5 >= last_used_at + interval '5 minutes')`

// apiKeyUsageIdentity is one credential instance. Digest alone cannot fence a
// delayed sample: rotation A to B and back to A, and revoke then reactivate,
// both reuse the digest but advance the generation.
type apiKeyUsageIdentity struct {
	workspaceID workspace.ID
	keyID       string
	digest      [sha256.Size]byte
	generation  int64
}

// apiKeyUsageObservation is captured from one committed admission. observedAt
// is the database clock read by the admission's post-lock recheck.
type apiKeyUsageObservation struct {
	identity   apiKeyUsageIdentity
	observedAt time.Time
}

type apiKeyUsageEntry struct {
	observedAt   time.Time
	pending      bool
	sequence     uint64
	nextEligible time.Time
}

// APIKeyUsageRecorder owns Auth's one usage worker. Admission submits after its
// transaction commits and never waits: a contended map, a closed recorder or a
// new identity beyond capacity drops the observation and counts it. Each
// one-second tick writes at most 128 due identities, one transaction at a time
// on the borrowed Auth pool, and suppresses each written or attempted identity
// locally for five minutes. Close discards pending samples without flushing.
type APIKeyUsageRecorder struct {
	db     *sql.DB
	logger *slog.Logger

	closed   atomic.Bool
	mu       sync.Mutex
	entries  map[apiKeyUsageIdentity]*apiKeyUsageEntry
	sequence uint64
	cancel   context.CancelFunc
	done     chan struct{}

	droppedCapacity, droppedContended, droppedClosed atomic.Uint64
	droppedDeadline, droppedDatabase                 atomic.Uint64
	updated                                          atomic.Uint64

	// In-package tests replace these before Start. now is the local monotonic
	// scheduling clock; it never supplies a stored usage time.
	now               func() time.Time
	ticks             <-chan time.Time
	beforeTransaction func(context.Context)
}

// NewAPIKeyUsageRecorder borrows Auth's pool and accepts submissions. Its
// worker writes nothing until Start.
func NewAPIKeyUsageRecorder(db *sql.DB, logger *slog.Logger) *APIKeyUsageRecorder {
	return &APIKeyUsageRecorder{db: db, logger: logger, entries: map[apiKeyUsageIdentity]*apiKeyUsageEntry{}, now: time.Now}
}

// Start runs the worker under the application lifecycle context. Cancelling
// that context also closes the recorder; Close still joins the worker.
func (r *APIKeyUsageRecorder) Start(ctx context.Context) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed.Load() || r.done != nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	r.cancel, r.done = cancel, make(chan struct{})
	ticks := r.ticks
	var ticker *time.Ticker
	if ticks == nil {
		ticker = time.NewTicker(apiKeyUsageTickInterval)
		ticks = ticker.C
	}
	go func(done chan struct{}) {
		defer close(done)
		if ticker != nil {
			defer ticker.Stop()
		}
		defer r.discard()
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-ticks:
				if !ok {
					return
				}
			}
			r.flush(ctx)
		}
	}(r.done)
}

// Close marks submissions closed, cancels the worker, discards pending samples
// and joins the worker. It never flushes, so shutdown does not wait for
// best-effort writes. Close is idempotent and runs before the pool closes.
func (r *APIKeyUsageRecorder) Close() {
	if r == nil {
		return
	}
	r.closed.Store(true)
	r.mu.Lock()
	cancel, done := r.cancel, r.done
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	r.discard()
	if done != nil {
		<-done
	}
}

func (r *APIKeyUsageRecorder) discard() {
	r.closed.Store(true)
	r.mu.Lock()
	r.entries = nil
	r.mu.Unlock()
}

// submit records one committed admission without waiting for the worker or
// the database. It tries the map mutex exactly once.
func (r *APIKeyUsageRecorder) submit(observation apiKeyUsageObservation) {
	if r == nil {
		return
	}
	if r.closed.Load() {
		r.droppedClosed.Add(1)
		return
	}
	if !r.mu.TryLock() {
		r.droppedContended.Add(1)
		return
	}
	defer r.mu.Unlock()
	if r.closed.Load() {
		r.droppedClosed.Add(1)
		return
	}
	if entry, ok := r.entries[observation.identity]; ok {
		if observation.observedAt.After(entry.observedAt) {
			entry.observedAt = observation.observedAt
		}
		entry.pending = true
		return
	}
	// A new identity never evicts an older one: without evidence to prefer
	// either, capacity pressure drops the newcomer.
	if len(r.entries) >= apiKeyUsageCapacity {
		r.droppedCapacity.Add(1)
		return
	}
	r.sequence++
	r.entries[observation.identity] = &apiKeyUsageEntry{observedAt: observation.observedAt, pending: true, sequence: r.sequence, nextEligible: r.now()}
}

type apiKeyUsageWork struct {
	identity   apiKeyUsageIdentity
	observedAt time.Time
}

// takeDue forgets identities whose suppression has ended without a new
// sample, then claims up to one batch of due pending identities, oldest
// eligibility first. A claimed identity is suppressed for five minutes whether
// or not its write later succeeds; a sample arriving meanwhile marks it pending
// again with the newest observation and is written after suppression ends.
func (r *APIKeyUsageRecorder) takeDue() []apiKeyUsageWork {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	type candidate struct {
		identity apiKeyUsageIdentity
		entry    *apiKeyUsageEntry
	}
	var due []candidate
	for identity, entry := range r.entries {
		if now.Before(entry.nextEligible) {
			continue
		}
		if !entry.pending {
			delete(r.entries, identity)
			continue
		}
		due = append(due, candidate{identity: identity, entry: entry})
	}
	slices.SortFunc(due, func(a, b candidate) int {
		if order := a.entry.nextEligible.Compare(b.entry.nextEligible); order != 0 {
			return order
		}
		return cmp.Compare(a.entry.sequence, b.entry.sequence)
	})
	if len(due) > apiKeyUsageBatchSize {
		due = due[:apiKeyUsageBatchSize]
	}
	work := make([]apiKeyUsageWork, len(due))
	for i, c := range due {
		work[i] = apiKeyUsageWork{identity: c.identity, observedAt: c.entry.observedAt}
		c.entry.pending = false
		c.entry.nextEligible = now.Add(apiKeyUsageSuppression)
	}
	return work
}

// flush writes one batch within a one-second total deadline. Work not started
// by the deadline is dropped as "deadline"; an attempted write that fails is
// dropped as "database". Lifecycle cancellation abandons the batch silently.
func (r *APIKeyUsageRecorder) flush(ctx context.Context) {
	work := r.takeDue()
	if len(work) == 0 {
		return
	}
	batch, cancel := context.WithTimeout(ctx, apiKeyUsageBatchDeadline)
	defer cancel()
	failed := 0
	for i, item := range work {
		if r.beforeTransaction != nil {
			r.beforeTransaction(batch)
		}
		if batch.Err() != nil {
			if ctx.Err() != nil {
				return
			}
			r.droppedDeadline.Add(uint64(len(work) - i))
			break
		}
		written, err := r.write(batch, item)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			failed++
			r.droppedDatabase.Add(1)
			continue
		}
		if written {
			r.updated.Add(1)
		}
	}
	if failed > 0 && r.logger != nil {
		r.logger.Warn("auth.api_key_usage.failed", "phase", "api_key_usage", "error.class", "dependency_unavailable", "error.code", "auth_api_key_usage_unavailable", "error.message_safe", "api key usage update unavailable", "failed.count", failed)
	}
}

// write is one transaction under the sample's trusted workspace. Its
// deadline covers pool acquisition, workspace setting, the update, commit and
// rollback.
func (r *APIKeyUsageRecorder) write(ctx context.Context, item apiKeyUsageWork) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, apiKeyUsageTransactionDeadline)
	defer cancel()
	var rows int64
	err := storage.WithWorkspaceTx(ctx, r.db, string(item.identity.workspaceID), func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, apiKeyUsageUpdate, string(item.identity.workspaceID), item.identity.keyID, item.identity.digest[:], item.identity.generation, item.observedAt)
		if err != nil {
			return err
		}
		rows, err = result.RowsAffected()
		return err
	})
	return rows == 1, err
}

// Collector reports fixed-label aggregate counters without taking the map
// mutex, so scrapes never contend with submissions.
func (r *APIKeyUsageRecorder) Collector() workload.MetricsCollector {
	return func(context.Context) ([]workload.Metric, error) {
		if r == nil {
			return nil, nil
		}
		const submissions, samples = "API-key usage submissions dropped before reaching the worker.", "API-key usage samples the worker selected but did not write."
		reason := func(value string) []workload.MetricLabel {
			return []workload.MetricLabel{{Name: "reason", Value: value}}
		}
		return []workload.Metric{
			{Name: "tetral_auth_api_key_usage_submissions_dropped_total", Help: submissions, Type: "counter", Labels: reason("capacity"), Value: float64(r.droppedCapacity.Load())},
			{Name: "tetral_auth_api_key_usage_submissions_dropped_total", Help: submissions, Type: "counter", Labels: reason("contended"), Value: float64(r.droppedContended.Load())},
			{Name: "tetral_auth_api_key_usage_submissions_dropped_total", Help: submissions, Type: "counter", Labels: reason("closed"), Value: float64(r.droppedClosed.Load())},
			{Name: "tetral_auth_api_key_usage_samples_dropped_total", Help: samples, Type: "counter", Labels: reason("deadline"), Value: float64(r.droppedDeadline.Load())},
			{Name: "tetral_auth_api_key_usage_samples_dropped_total", Help: samples, Type: "counter", Labels: reason("database"), Value: float64(r.droppedDatabase.Load())},
			{Name: "tetral_auth_api_key_usage_rows_updated_total", Help: "API-key rows whose last_used_at this process updated.", Type: "counter", Value: float64(r.updated.Load())},
		}, nil
	}
}
