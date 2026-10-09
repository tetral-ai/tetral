package auth

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// usageClock is the recorder's local monotonic scheduling clock under test.
type usageClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *usageClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *usageClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// testUsageRecorder returns an unstarted recorder with a controlled clock.
// Tests run its tick body, flush, directly; lifecycle tests start the actual
// worker loop with a fake tick channel instead.
func testUsageRecorder(db *sql.DB, logger *slog.Logger) (*APIKeyUsageRecorder, *usageClock) {
	clock := &usageClock{now: time.Now()}
	r := NewAPIKeyUsageRecorder(db, logger)
	r.now = clock.Now
	return r, clock
}

func usageEntries(r *APIKeyUsageRecorder) map[apiKeyUsageIdentity]apiKeyUsageEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	entries := map[apiKeyUsageIdentity]apiKeyUsageEntry{}
	for identity, entry := range r.entries {
		entries[identity] = *entry
	}
	return entries
}

func usageIdentity(ws workspace.ID, id, raw string, generation int64) apiKeyUsageIdentity {
	return apiKeyUsageIdentity{workspaceID: ws, keyID: id, digest: sha256.Sum256([]byte(raw)), generation: generation}
}

type keyUsageRow struct {
	lastUsed   sql.NullTime
	generation int64
}

func keyUsage(t *testing.T, admin *sql.DB, id string) keyUsageRow {
	t.Helper()
	var row keyUsageRow
	if err := admin.QueryRow(`SELECT last_used_at,usage_generation FROM api_keys WHERE id=$1`, id).Scan(&row.lastUsed, &row.generation); err != nil {
		t.Fatal(err)
	}
	return row
}

// sqlPause holds the first pool statement starting with match, before it is
// sent or after it completes. Unless honor is set, the pause outlives the
// statement's own deadline, so a transaction can be held deliberately.
type sqlPause struct {
	match   string
	end     bool
	honor   bool
	claimed atomic.Bool
	entered chan sqlPaused
	release chan struct{}
	once    sync.Once
}

type sqlPaused struct {
	pid uint32
	ctx context.Context
}

func newSQLPause(match string, end, honor bool) *sqlPause {
	return &sqlPause{match: match, end: end, honor: honor, entered: make(chan sqlPaused, 1), release: make(chan struct{})}
}

func (p *sqlPause) resume() { p.once.Do(func() { close(p.release) }) }

func (p *sqlPause) wait(ctx context.Context, t *testing.T) sqlPaused {
	t.Helper()
	select {
	case paused := <-p.entered:
		return paused
	case <-ctx.Done():
		t.Fatalf("statement %q never reached its pause", p.match)
		return sqlPaused{}
	}
}

func (p *sqlPause) hold(ctx context.Context, conn *pgx.Conn) {
	if !p.claimed.CompareAndSwap(false, true) {
		return
	}
	p.entered <- sqlPaused{pid: conn.PgConn().PID(), ctx: ctx}
	if p.honor {
		select {
		case <-p.release:
		case <-ctx.Done():
		}
		return
	}
	<-p.release
}

// sqlTrace records every statement sent on its pool and applies its pauses.
type sqlTrace struct {
	pauses     []*sqlPause
	mu         sync.Mutex
	statements []string
}

type sqlTracePauseKey struct{}

func newSQLTrace(t *testing.T, pauses ...*sqlPause) *sqlTrace {
	for _, pause := range pauses {
		t.Cleanup(pause.resume)
	}
	return &sqlTrace{pauses: pauses}
}

func (s *sqlTrace) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	s.mu.Lock()
	s.statements = append(s.statements, data.SQL)
	s.mu.Unlock()
	for _, pause := range s.pauses {
		if !strings.HasPrefix(data.SQL, pause.match) {
			continue
		}
		if pause.end {
			return context.WithValue(ctx, sqlTracePauseKey{}, pause)
		}
		pause.hold(ctx, conn)
	}
	return ctx
}

func (s *sqlTrace) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, _ pgx.TraceQueryEndData) {
	if pause, ok := ctx.Value(sqlTracePauseKey{}).(*sqlPause); ok {
		pause.hold(ctx, conn)
	}
}

func (s *sqlTrace) count(prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, statement := range s.statements {
		if strings.HasPrefix(statement, prefix) {
			n++
		}
	}
	return n
}

func (s *sqlTrace) all() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.statements...)
}

// sent counts statements other than driver liveness probes.
func (s *sqlTrace) sent() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, statement := range s.statements {
		if !strings.HasPrefix(statement, "--") {
			n++
		}
	}
	return n
}

const usageUpdatePrefix = "UPDATE public.api_keys"

func usageCounters(r *APIKeyUsageRecorder) map[string]uint64 {
	return map[string]uint64{
		"capacity": r.droppedCapacity.Load(), "contended": r.droppedContended.Load(), "closed": r.droppedClosed.Load(),
		"deadline": r.droppedDeadline.Load(), "database": r.droppedDatabase.Load(), "updated": r.updated.Load(),
	}
}

func requireUsageCounters(t *testing.T, r *APIKeyUsageRecorder, want map[string]uint64) {
	t.Helper()
	got := usageCounters(r)
	for name, value := range got {
		if value != want[name] {
			t.Fatalf("usage counters=%v want=%v", got, want)
		}
	}
}

func awaitKeyUsage(ctx context.Context, t *testing.T, admin *sql.DB, id string) time.Time {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if row := keyUsage(t, admin, id); row.lastUsed.Valid {
			return row.lastUsed.Time
		}
		select {
		case <-ctx.Done():
			t.Fatal("usage worker did not write the submitted sample")
		case <-ticker.C:
		}
	}
}

func TestAPIKeyUsageWorkerAndAdmissionContention(t *testing.T) {
	t.Run("WorkerPausedBeforeTransactionHoldsNoCredentialLock", func(t *testing.T) {
		admin, _, store, _ := authorityFixture(t)
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		key, err := seedIndependentKeyForTest(ctx, store.db, workspace.DefaultID, "paused worker")
		if err != nil {
			t.Fatal(err)
		}
		r, _ := testUsageRecorder(store.db, nil)
		if _, err := NewAuthorityResolver(store.db, workspace.DefaultID, r).AuthenticateKey(ctx, key.APIKey); err != nil {
			t.Fatal(err)
		}
		paused, release := make(chan struct{}), make(chan struct{})
		r.beforeTransaction = func(context.Context) { close(paused); <-release }
		flushed := make(chan struct{})
		go func() { defer close(flushed); r.flush(ctx) }()
		t.Cleanup(func() {
			select {
			case <-release:
			default:
				close(release)
			}
			<-flushed
		})
		<-paused
		admission, stop := context.WithTimeout(ctx, 2*time.Second)
		defer stop()
		if _, err := NewAuthorityResolver(store.db, workspace.DefaultID, nil).AuthenticateKey(admission, key.APIKey); err != nil {
			t.Fatalf("admission waited on a worker that had not begun its transaction: %T", err)
		}
		close(release)
		<-flushed
		if !keyUsage(t, admin, key.ID).lastUsed.Valid {
			t.Fatal("released worker did not write the sample")
		}
	})
	t.Run("WorkerUpdateOnAnotherKeyLeavesAdmissionAConnection", func(t *testing.T) {
		admin, _, store, _ := authorityFixture(t)
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		busy, err := seedIndependentKeyForTest(ctx, store.db, workspace.DefaultID, "worker key")
		if err != nil {
			t.Fatal(err)
		}
		other, err := seedIndependentKeyForTest(ctx, store.db, workspace.DefaultID, "admitted key")
		if err != nil {
			t.Fatal(err)
		}
		update := newSQLPause(usageUpdatePrefix, true, false)
		pool := storagetest.OpenRuntimeRoleDBWithTracer(t, store.db, newSQLTrace(t, update))
		r, _ := testUsageRecorder(pool, nil)
		if _, err := NewAuthorityResolver(pool, workspace.DefaultID, r).AuthenticateKey(ctx, busy.APIKey); err != nil {
			t.Fatal(err)
		}
		flushed := make(chan struct{})
		go func() { defer close(flushed); r.flush(ctx) }()
		t.Cleanup(func() { update.resume(); <-flushed })
		worker := update.wait(ctx, t)
		// The worker owns one pooled connection and the other key's row lock.
		admission, stop := context.WithTimeout(ctx, 2*time.Second)
		defer stop()
		if _, err := NewAuthorityResolver(pool, workspace.DefaultID, nil).AuthenticateKey(admission, other.APIKey); err != nil {
			t.Fatalf("unrelated admission waited on the worker transaction: %T", err)
		}
		var blocked int
		if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid))`, int(worker.pid)).Scan(&blocked); err != nil || blocked != 0 {
			t.Fatalf("worker transaction blocked %d backends", blocked)
		}
	})
	t.Run("SameKeyAdmissionWaitsUntilCancelledWorkerRollsBack", func(t *testing.T) {
		admin, _, store, _ := authorityFixture(t)
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		key, err := seedIndependentKeyForTest(ctx, store.db, workspace.DefaultID, "contended key")
		if err != nil {
			t.Fatal(err)
		}
		update := newSQLPause(usageUpdatePrefix, true, false)
		pool := storagetest.OpenRuntimeRoleDBWithTracer(t, store.db, newSQLTrace(t, update))
		r, _ := testUsageRecorder(pool, nil)
		if _, err := NewAuthorityResolver(pool, workspace.DefaultID, r).AuthenticateKey(ctx, key.APIKey); err != nil {
			t.Fatal(err)
		}
		flushed := make(chan struct{})
		go func() { defer close(flushed); r.flush(ctx) }()
		t.Cleanup(func() { update.resume(); <-flushed })
		worker := update.wait(ctx, t)
		admitted := make(chan error, 1)
		go func() {
			_, err := NewAuthorityResolver(store.db, workspace.DefaultID, nil).AuthenticateKey(ctx, key.APIKey)
			admitted <- err
		}()
		awaitAuthorityBlock(ctx, t, admin, int(worker.pid))
		// The worker's 100 ms transaction deadline expires while it holds the
		// updated row. The real driver then fails commit, closes the connection
		// and PostgreSQL rolls back, releasing the admission's share lock wait.
		select {
		case <-worker.ctx.Done():
		case <-ctx.Done():
			t.Fatal("worker transaction outlived its deadline")
		}
		select {
		case err := <-admitted:
			t.Fatalf("admission passed a held worker update: %v", err)
		default:
		}
		update.resume()
		<-flushed
		select {
		case err := <-admitted:
			if err != nil {
				t.Fatalf("admission failed after worker rollback: %T", err)
			}
		case <-ctx.Done():
			t.Fatal("cancelled worker kept the credential row locked")
		}
		if keyUsage(t, admin, key.ID).lastUsed.Valid {
			t.Fatal("cancelled worker transaction committed usage")
		}
		requireUsageCounters(t, r, map[string]uint64{"database": 1})
	})
	t.Run("SaturatedPoolAcquisitionTimesOut", func(t *testing.T) {
		admin, _, store, _ := authorityFixture(t)
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		pool := storagetest.OpenRuntimeRoleDBWithTracer(t, store.db, nil)
		pool.SetMaxOpenConns(1)
		r, _ := testUsageRecorder(pool, nil)
		var keys []string
		for _, name := range []string{"saturated pool a", "saturated pool b", "saturated pool c"} {
			key, err := seedIndependentKeyForTest(ctx, store.db, workspace.DefaultID, name)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := NewAuthorityResolver(store.db, workspace.DefaultID, r).AuthenticateKey(ctx, key.APIKey); err != nil {
				t.Fatal(err)
			}
			keys = append(keys, key.ID)
		}
		conn, err := pool.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		r.flush(ctx)
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
		// Each attempt gives up at its own 100 ms transaction deadline, so all
		// three fail as database drops inside the one-second batch; waiting for
		// the batch deadline instead would leave the later two as deadline drops.
		requireUsageCounters(t, r, map[string]uint64{"database": 3})
		for _, id := range keys {
			if keyUsage(t, admin, id).lastUsed.Valid {
				t.Fatal("timed-out pool acquisition wrote usage")
			}
		}
	})
	t.Run("SubmittedSampleOutlivesRequestButNotApplication", func(t *testing.T) {
		admin, _, store, _ := authorityFixture(t)
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		first, err := seedIndependentKeyForTest(ctx, store.db, workspace.DefaultID, "request cancelled")
		if err != nil {
			t.Fatal(err)
		}
		second, err := seedIndependentKeyForTest(ctx, store.db, workspace.DefaultID, "application cancelled")
		if err != nil {
			t.Fatal(err)
		}
		third, err := seedIndependentKeyForTest(ctx, store.db, workspace.DefaultID, "pending at shutdown")
		if err != nil {
			t.Fatal(err)
		}
		var logs bytes.Buffer
		r, _ := testUsageRecorder(store.db, slog.New(slog.NewJSONHandler(&logs, nil)))
		ticks := make(chan time.Time)
		r.ticks = ticks
		var pauseBatch atomic.Bool
		paused := make(chan context.Context, 1)
		r.beforeTransaction = func(batch context.Context) {
			if pauseBatch.Load() {
				paused <- batch
				<-batch.Done()
			}
		}
		app, stopApp := context.WithCancel(ctx)
		defer stopApp()
		r.Start(app)
		t.Cleanup(r.Close)
		resolver := NewAuthorityResolver(store.db, workspace.DefaultID, r)
		request, endRequest := context.WithCancel(ctx)
		if _, err := resolver.AuthenticateKey(request, first.APIKey); err != nil {
			t.Fatal(err)
		}
		endRequest()
		ticks <- time.Now()
		awaitKeyUsage(ctx, t, admin, first.ID)

		if _, err := resolver.AuthenticateKey(ctx, second.APIKey); err != nil {
			t.Fatal(err)
		}
		pauseBatch.Store(true)
		ticks <- time.Now()
		batch := <-paused
		if _, err := resolver.AuthenticateKey(ctx, third.APIKey); err != nil {
			t.Fatal(err)
		}
		pauseBatch.Store(false)
		stopApp()
		<-batch.Done()
		if !errors.Is(batch.Err(), context.Canceled) {
			t.Fatalf("worker batch ended by %v, not application cancellation", batch.Err())
		}
		// Application cancellation alone ends the worker and closes submissions,
		// before any explicit Close.
		<-r.done
		if keyUsage(t, admin, second.ID).lastUsed.Valid {
			t.Fatal("application cancellation did not cancel the submitted sample")
		}
		if keyUsage(t, admin, third.ID).lastUsed.Valid || len(usageEntries(r)) != 0 {
			t.Fatal("worker exit flushed or retained a pending sample")
		}
		r.submit(apiKeyUsageObservation{identity: usageIdentity(workspace.DefaultID, second.ID, second.APIKey, 1), observedAt: time.Now()})
		r.Close()
		requireUsageCounters(t, r, map[string]uint64{"closed": 1, "updated": 1})
		if logs.Len() != 0 {
			t.Fatal("shutdown cancellation produced a usage warning")
		}
	})
}

func TestAPIKeyUsageSamplesByDatabaseObservationAcrossRecorders(t *testing.T) {
	admin, _, store, _ := authorityFixture(t)
	ctx := context.Background()
	forward, err := seedIndependentKeyForTest(ctx, store.db, workspace.DefaultID, "forward arrival")
	if err != nil {
		t.Fatal(err)
	}
	reverse, err := seedIndependentKeyForTest(ctx, store.db, workspace.DefaultID, "reverse arrival")
	if err != nil {
		t.Fatal(err)
	}
	// Two replicas: independent recorders and pools of the actual Auth role.
	first, firstClock := testUsageRecorder(store.db, nil)
	second, secondClock := testUsageRecorder(storagetest.OpenRuntimeRoleDBWithTracer(t, store.db, nil), nil)
	base := dbClock(t, admin).Truncate(time.Microsecond)
	sample := func(r *APIKeyUsageRecorder, key *CreateAPIKeyResult, at time.Duration) time.Time {
		t.Helper()
		r.submit(apiKeyUsageObservation{identity: usageIdentity(workspace.DefaultID, key.ID, key.APIKey, 1), observedAt: base.Add(at)})
		r.flush(ctx)
		row := keyUsage(t, admin, key.ID)
		if !row.lastUsed.Valid {
			t.Fatal("sample left last_used_at empty")
		}
		return row.lastUsed.Time
	}
	if got := sample(first, forward, 0); !got.Equal(base) {
		t.Fatalf("first sample stored %s", got)
	}
	if got := sample(second, forward, 5*time.Minute-time.Millisecond); !got.Equal(base) {
		t.Fatalf("sample inside the five-minute interval stored %s", got)
	}
	firstClock.Advance(apiKeyUsageSuppression)
	if got := sample(first, forward, 5*time.Minute); !got.Equal(base.Add(5 * time.Minute)) {
		t.Fatalf("boundary sample stored %s", got)
	}
	if got := sample(first, reverse, 5*time.Minute); !got.Equal(base.Add(5 * time.Minute)) {
		t.Fatalf("later-arriving-first sample stored %s", got)
	}
	if got := sample(second, reverse, 0); !got.Equal(base.Add(5 * time.Minute)) {
		t.Fatalf("earlier observation moved last_used_at backward to %s", got)
	}
	secondClock.Advance(apiKeyUsageSuppression)
	if got := sample(second, reverse, 5*time.Minute-time.Millisecond); !got.Equal(base.Add(5 * time.Minute)) {
		t.Fatalf("earlier observation moved last_used_at backward to %s", got)
	}
	if first.updated.Load() != 3 || second.updated.Load() != 0 {
		t.Fatalf("updated rows first=%d second=%d", first.updated.Load(), second.updated.Load())
	}
}

func TestAPIKeyUsageGenerationFencesCredentialInstances(t *testing.T) {
	admin, _, store, _ := authorityFixture(t)
	ctx := context.Background()
	rawA, err := GenerateAPIKey()
	if err != nil {
		t.Fatal(err)
	}
	rawB, err := GenerateAPIKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBootstrap(ctx, workspace.DefaultID, rawA); err != nil {
		t.Fatal(err)
	}
	var keyID string
	if err := admin.QueryRow(`SELECT id FROM api_keys WHERE key_kind='bootstrap'`).Scan(&keyID); err != nil {
		t.Fatal(err)
	}
	r, clock := testUsageRecorder(store.db, nil)
	resolver := NewAuthorityResolver(store.db, workspace.DefaultID, r)
	admit := func() {
		t.Helper()
		if _, err := resolver.AuthenticateKey(ctx, rawA); err != nil {
			t.Fatal(err)
		}
	}
	requireRow := func(generation int64, used bool) keyUsageRow {
		t.Helper()
		row := keyUsage(t, admin, keyID)
		if row.generation != generation || row.lastUsed.Valid != used {
			t.Fatalf("bootstrap row generation=%d used=%t want generation=%d used=%t", row.generation, row.lastUsed.Valid, generation, used)
		}
		return row
	}

	admit() // captured under generation 1, then delayed
	for _, raw := range []string{rawB, rawA} {
		if err := store.UpsertBootstrap(ctx, workspace.DefaultID, raw); err != nil {
			t.Fatal(err)
		}
	}
	requireRow(3, false)
	r.flush(ctx)
	requireRow(3, false)
	admit()
	r.flush(ctx)
	written := requireRow(3, true)
	if err := store.UpsertBootstrap(ctx, workspace.DefaultID, rawA); err != nil {
		t.Fatal(err)
	}
	if row := requireRow(3, true); !row.lastUsed.Time.Equal(written.lastUsed.Time) {
		t.Fatal("same active bootstrap refresh reset usage")
	}

	admit() // a newer generation-3 sample, delayed across revoke and reactivation
	if err := store.RevokeForWorkspace(ctx, workspace.DefaultID, keyID); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBootstrap(ctx, workspace.DefaultID, rawA); err != nil {
		t.Fatal(err)
	}
	requireRow(5, false)
	clock.Advance(apiKeyUsageSuppression)
	r.flush(ctx)
	requireRow(5, false)
	if r.updated.Load() != 1 {
		t.Fatalf("stale generations updated rows: %d", r.updated.Load())
	}
	admit()
	r.flush(ctx)
	current := requireRow(5, true)

	// Only the exact workspace, ID, digest and generation can update the row,
	// even with an observation well past the sampling interval.
	if _, err := workspace.NewSeeder(admin).Seed(ctx, "ws_usage_foreign", "foreign"); err != nil {
		t.Fatal(err)
	}
	later := current.lastUsed.Time.Add(time.Hour)
	for _, identity := range []apiKeyUsageIdentity{
		usageIdentity("ws_usage_foreign", keyID, rawA, 5),
		usageIdentity(workspace.DefaultID, keyID, rawB, 5),
		usageIdentity(workspace.DefaultID, "ak_usage_missing", rawA, 5),
		usageIdentity(workspace.DefaultID, keyID, rawA, 4),
	} {
		r.submit(apiKeyUsageObservation{identity: identity, observedAt: later})
	}
	r.flush(ctx)
	if row := requireRow(5, true); !row.lastUsed.Time.Equal(current.lastUsed.Time) || r.updated.Load() != 2 {
		t.Fatal("a sample for another workspace, digest, key or generation updated the row")
	}

	// Callers cannot choose a generation, and exhaustion rejects the change.
	if _, err := admin.Exec(`UPDATE api_keys SET usage_generation=1,name=name WHERE id=$1`, keyID); err != nil {
		t.Fatal(err)
	}
	requireRow(5, true)
	if _, err := admin.Exec(`INSERT INTO api_keys(id,workspace_id,name,key_prefix,key_digest,key_kind,authority_kind,created_at,usage_generation) VALUES('ak_usage_exhausted',$1,'exhausted generation','fixture',$2,'standard','independent_key',clock_timestamp(),9223372036854775807)`, string(workspace.DefaultID), DigestAPIKey("test-only-exhausted-generation")); err != nil {
		t.Fatal(err)
	}
	var pgErr *pgconn.PgError
	if err := store.RevokeForWorkspace(ctx, workspace.DefaultID, "ak_usage_exhausted"); !errors.As(err, &pgErr) || pgErr.Code != "22003" {
		t.Fatalf("exhausted generation revoke: %v", err)
	}
	var revoked bool
	if err := admin.QueryRow(`SELECT revoked_at IS NOT NULL FROM api_keys WHERE id='ak_usage_exhausted'`).Scan(&revoked); err != nil || revoked {
		t.Fatal("generation overflow did not reject the revoke")
	}
}

func TestAPIKeyUsageRecorderBounds(t *testing.T) {
	t.Run("CapacityContentionAndClose", func(t *testing.T) {
		r, _ := testUsageRecorder(nil, nil)
		at := time.Now()
		for i := range apiKeyUsageCapacity {
			r.submit(apiKeyUsageObservation{identity: usageIdentity(workspace.DefaultID, fmt.Sprintf("ak_%d", i), "raw", 1), observedAt: at})
		}
		excess := usageIdentity(workspace.DefaultID, "ak_excess", "raw", 1)
		r.submit(apiKeyUsageObservation{identity: excess, observedAt: at})
		retained := usageIdentity(workspace.DefaultID, "ak_0", "raw", 1)
		r.submit(apiKeyUsageObservation{identity: retained, observedAt: at.Add(time.Minute)})
		r.submit(apiKeyUsageObservation{identity: retained, observedAt: at})
		entries := usageEntries(r)
		if _, ok := entries[excess]; ok || len(entries) != apiKeyUsageCapacity {
			t.Fatalf("capacity admitted a new identity: %d entries", len(entries))
		}
		if entry := entries[retained]; !entry.pending || !entry.observedAt.Equal(at.Add(time.Minute)) {
			t.Fatal("full map did not coalesce an existing identity to its newest observation")
		}
		r.mu.Lock()
		r.submit(apiKeyUsageObservation{identity: retained, observedAt: at.Add(time.Hour)})
		r.mu.Unlock()
		if entry := usageEntries(r)[retained]; !entry.observedAt.Equal(at.Add(time.Minute)) {
			t.Fatal("contended submission waited for the map")
		}
		r.Close()
		r.Close()
		r.submit(apiKeyUsageObservation{identity: retained, observedAt: at})
		requireUsageCounters(t, r, map[string]uint64{"capacity": 1, "contended": 1, "closed": 1})
	})
	t.Run("BatchLimitOrderAndEmptyTicks", func(t *testing.T) {
		_, _, store, _ := authorityFixture(t)
		ctx := context.Background()
		trace := newSQLTrace(t)
		r, clock := testUsageRecorder(storagetest.OpenRuntimeRoleDBWithTracer(t, store.db, trace), nil)
		at := time.Now()
		ids := make([]apiKeyUsageIdentity, apiKeyUsageBatchSize+2)
		for i := range ids {
			ids[i] = usageIdentity(workspace.DefaultID, fmt.Sprintf("ak_batch_%03d", i), "raw", 1)
			r.submit(apiKeyUsageObservation{identity: ids[i], observedAt: at})
			clock.Advance(time.Millisecond)
		}
		flushAndCount := func() int {
			before := trace.count(usageUpdatePrefix)
			r.flush(ctx)
			return trace.count(usageUpdatePrefix) - before
		}
		if n := flushAndCount(); n != apiKeyUsageBatchSize {
			t.Fatalf("one tick attempted %d samples", n)
		}
		entries := usageEntries(r)
		if entries[ids[apiKeyUsageBatchSize-1]].pending || !entries[ids[apiKeyUsageBatchSize]].pending {
			t.Fatal("tick did not take the oldest eligible identities first")
		}
		if n := flushAndCount(); n != 2 {
			t.Fatalf("second tick attempted %d samples", n)
		}
		statements := trace.sent()
		r.flush(ctx)
		clock.Advance(apiKeyUsageSuppression)
		r.flush(ctx)
		if trace.sent() != statements || len(usageEntries(r)) != 0 {
			t.Fatal("ticks with nothing due issued SQL or kept expired idle identities")
		}
		r.flush(ctx)
		if trace.sent() != statements {
			t.Fatal("empty map tick issued SQL")
		}
	})
	t.Run("EqualEligibilityTakesInsertionOrder", func(t *testing.T) {
		_, _, store, _ := authorityFixture(t)
		r, _ := testUsageRecorder(storagetest.OpenRuntimeRoleDBWithTracer(t, store.db, nil), nil)
		at := time.Now()
		ids := make([]apiKeyUsageIdentity, apiKeyUsageBatchSize+2)
		for i := range ids {
			// The fake clock does not move, so every identity becomes eligible at
			// the same instant and only insertion order separates them.
			ids[i] = usageIdentity(workspace.DefaultID, fmt.Sprintf("ak_tie_%03d", i), "raw", 1)
			r.submit(apiKeyUsageObservation{identity: ids[i], observedAt: at})
		}
		r.flush(context.Background())
		entries := usageEntries(r)
		for i, identity := range ids {
			if pending := entries[identity].pending; pending != (i >= apiKeyUsageBatchSize) {
				t.Fatalf("identity %d pending=%v after one tick", i, pending)
			}
		}
	})
	t.Run("CoalescingDuringFlush", func(t *testing.T) {
		admin, _, store, _ := authorityFixture(t)
		ctx := context.Background()
		key, err := seedIndependentKeyForTest(ctx, store.db, workspace.DefaultID, "coalesced")
		if err != nil {
			t.Fatal(err)
		}
		r, clock := testUsageRecorder(store.db, nil)
		identity := usageIdentity(workspace.DefaultID, key.ID, key.APIKey, 1)
		base := dbClock(t, admin).Truncate(time.Microsecond)
		r.submit(apiKeyUsageObservation{identity: identity, observedAt: base})
		r.beforeTransaction = func(context.Context) {
			r.beforeTransaction = nil
			r.submit(apiKeyUsageObservation{identity: identity, observedAt: base.Add(6 * time.Minute)})
			r.submit(apiKeyUsageObservation{identity: identity, observedAt: base.Add(time.Minute)})
		}
		r.flush(ctx)
		if row := keyUsage(t, admin, key.ID); !row.lastUsed.Time.Equal(base) {
			t.Fatal("in-flight sample changed")
		}
		if entry := usageEntries(r)[identity]; !entry.pending || !entry.observedAt.Equal(base.Add(6*time.Minute)) {
			t.Fatal("flush completion erased an observation that arrived during it")
		}
		clock.Advance(apiKeyUsageSuppression)
		r.flush(ctx)
		if row := keyUsage(t, admin, key.ID); !row.lastUsed.Time.Equal(base.Add(6 * time.Minute)) {
			t.Fatal("coalesced newest observation was not written after suppression")
		}
	})
	t.Run("DeadlineCutoffKeepsSuppression", func(t *testing.T) {
		_, _, store, _ := authorityFixture(t)
		ctx := context.Background()
		trace := newSQLTrace(t)
		r, clock := testUsageRecorder(storagetest.OpenRuntimeRoleDBWithTracer(t, store.db, trace), nil)
		at := time.Now()
		ids := []apiKeyUsageIdentity{usageIdentity(workspace.DefaultID, "ak_deadline_a", "raw", 1), usageIdentity(workspace.DefaultID, "ak_deadline_b", "raw", 1), usageIdentity(workspace.DefaultID, "ak_deadline_c", "raw", 1)}
		for _, identity := range ids {
			r.submit(apiKeyUsageObservation{identity: identity, observedAt: at})
			clock.Advance(time.Millisecond)
		}
		calls := 0
		r.beforeTransaction = func(batch context.Context) {
			if calls++; calls == 2 {
				<-batch.Done()
			}
		}
		r.flush(ctx)
		r.beforeTransaction = nil
		if n := trace.count(usageUpdatePrefix); n != 1 {
			t.Fatalf("attempted %d samples before the batch deadline", n)
		}
		requireUsageCounters(t, r, map[string]uint64{"deadline": 2})
		r.submit(apiKeyUsageObservation{identity: ids[1], observedAt: at.Add(time.Minute)})
		clock.Advance(time.Second)
		r.flush(ctx)
		if n := trace.count(usageUpdatePrefix); n != 1 {
			t.Fatal("deadline-dropped identity left its five-minute suppression")
		}
		clock.Advance(apiKeyUsageSuppression)
		r.flush(ctx)
		if n := trace.count(usageUpdatePrefix); n != 2 {
			t.Fatalf("fresh sample after suppression attempted %d times in total", n)
		}
	})
	t.Run("FailureLosesSampleAndKeepsSuppression", func(t *testing.T) {
		admin, _, store, _ := authorityFixture(t)
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		key, err := seedIndependentKeyForTest(ctx, store.db, workspace.DefaultID, "failed write")
		if err != nil {
			t.Fatal(err)
		}
		trace := newSQLTrace(t)
		var logs bytes.Buffer
		r, clock := testUsageRecorder(storagetest.OpenRuntimeRoleDBWithTracer(t, store.db, trace), slog.New(slog.NewJSONHandler(&logs, nil)))
		resolver := NewAuthorityResolver(store.db, workspace.DefaultID, r)
		if _, err := resolver.AuthenticateKey(ctx, key.APIKey); err != nil {
			t.Fatal(err)
		}
		held, _ := holdAuthorityRow(ctx, t, admin, "api_keys", key.ID)
		r.flush(ctx)
		if err := held.Rollback(); err != nil {
			t.Fatal(err)
		}
		requireUsageCounters(t, r, map[string]uint64{"database": 1})
		if _, err := resolver.AuthenticateKey(ctx, key.APIKey); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Second)
		r.flush(ctx)
		if trace.count(usageUpdatePrefix) != 1 || keyUsage(t, admin, key.ID).lastUsed.Valid {
			t.Fatal("failed identity retried inside its five-minute suppression")
		}
		clock.Advance(apiKeyUsageSuppression)
		r.flush(ctx)
		if !keyUsage(t, admin, key.ID).lastUsed.Valid {
			t.Fatal("fresh sample after suppression was not written")
		}
		var record map[string]any
		if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
			t.Fatalf("expected exactly one failure warning: %v", err)
		}
		if record["msg"] != "auth.api_key_usage.failed" || record["failed.count"] != float64(1) || record["error.class"] != "dependency_unavailable" {
			t.Fatalf("failure warning=%v", record)
		}
		for _, secret := range []string{key.ID, string(workspace.DefaultID), key.APIKey, hex.EncodeToString(DigestAPIKey(key.APIKey)), "context deadline", "canceling statement"} {
			if strings.Contains(logs.String(), secret) {
				t.Fatal("usage warning disclosed identity, credential or driver detail")
			}
		}
	})
}
