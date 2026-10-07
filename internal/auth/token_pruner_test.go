package auth

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/database"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func prunerFixture(t *testing.T) (*sql.DB, *storagetest.WorkloadDB, *AuthorityResolver) {
	t.Helper()
	admin := storagetest.NewPostgreSQLAdminDB(t)
	if _, err := NewPolicyStore(admin).Apply(context.Background(), policyFixture()); err != nil {
		t.Fatal(err)
	}
	w := storagetest.OpenWorkloadDB(t, admin, "auth")
	return admin, w, NewAuthorityResolver(w.DB, workspace.DefaultID)
}

func seedPruneToken(t *testing.T, db *sql.DB, label string, expiry time.Time, revoked bool) string {
	t.Helper()
	raw := "test-only-pruner-credential-" + label
	var revokedAt any
	if revoked {
		revokedAt = expiry
	}
	_, err := db.Exec(`INSERT INTO auth_access_tokens(id,workspace_id,token_digest,federation_rule_id,identity_id,grant_id,rule_revision,identity_revision,grant_revision,role_version,created_at,expires_at,revoked_at)
 VALUES($1,$2,$3,'rule_policy','identity_policy','grant_policy',1,1,1,$4,$5,$6,$7)`, label, string(workspace.DefaultID), DigestAPIKey(raw), PolicyVersion, expiry.Add(-72*time.Hour), expiry, revokedAt)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func dbClock(t *testing.T, db *sql.DB) time.Time {
	t.Helper()
	var now time.Time
	if err := db.QueryRow(`SELECT clock_timestamp()`).Scan(&now); err != nil {
		t.Fatal(err)
	}
	return now
}

func pruneIDs(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`SELECT id FROM auth_access_tokens ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

func TestAuthTokenPrunerRetentionAndBatch(t *testing.T) {
	t.Run("ControlledDatabaseClockStrictRetention", func(t *testing.T) {
		admin, w, resolver := prunerFixture(t)
		// This private clone preserves the deployed function body except its one
		// clock expression. Equality cannot be observed reliably with wall time.
		var definition, metadata string
		if err := admin.QueryRow(`SELECT pg_get_functiondef(oid),ROW(proowner,prosecdef,proconfig,proacl)::text FROM pg_proc WHERE oid='public.tetral_auth_prune_tokens(integer)'::regprocedure`).Scan(&definition, &metadata); err != nil {
			t.Fatal(err)
		}
		const clock = "pg_catalog.clock_timestamp()"
		if strings.Count(definition, clock) != 1 {
			t.Fatal("canonical pruner does not have exactly one replaceable clock expression")
		}
		controlled := strings.Replace(definition, clock, `TIMESTAMPTZ '2026-10-04 00:00:00+00'`, 1)
		if _, err := admin.Exec(controlled); err != nil {
			t.Fatal(err)
		}
		var installed, afterMetadata string
		if err := admin.QueryRow(`SELECT pg_get_functiondef(oid),ROW(proowner,prosecdef,proconfig,proacl)::text FROM pg_proc WHERE oid='public.tetral_auth_prune_tokens(integer)'::regprocedure`).Scan(&installed, &afterMetadata); err != nil {
			t.Fatal(err)
		}
		if installed != controlled || metadata != afterMetadata {
			t.Fatal("controlled clock changed function body or owner/security/search-path/ACL beyond the single substitution")
		}
		cutoff := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
		seedPruneToken(t, admin, "at_before", cutoff.Add(-time.Microsecond), false)
		seedPruneToken(t, admin, "at_equal", cutoff, false)
		seedPruneToken(t, admin, "at_after", cutoff.Add(time.Microsecond), false)
		seedPruneToken(t, admin, "at_revoked_old", cutoff.Add(-time.Microsecond), true)
		seedPruneToken(t, admin, "at_revoked_recent", cutoff.Add(time.Hour), true)
		seedPruneToken(t, admin, "at_live", cutoff.Add(25*time.Hour), false)
		seedPruneToken(t, admin, "at_future", cutoff.Add(48*time.Hour), false)
		key, err := seedIndependentKeyForTest(context.Background(), w.DB, workspace.DefaultID, "pruner-surviving-independent-key")
		if err != nil {
			t.Fatal(err)
		}
		result, err := resolver.PruneTokens(context.Background(), 1000)
		if err != nil {
			t.Fatal(err)
		}
		if result.DeletedCount != 2 || result.ExpiredBacklog != 0 || result.OldestExpiry.Valid {
			t.Fatalf("strict retention result=%+v", result)
		}
		want := []string{"at_after", "at_equal", "at_future", "at_live", "at_revoked_recent"}
		if ids := pruneIDs(t, admin); !slices.Equal(ids, want) {
			t.Fatalf("retained token IDs=%v want=%v", ids, want)
		}
		for _, table := range []string{"auth_federation_rules", "auth_identities", "auth_workspace_grants", "api_keys"} {
			var count int
			if err := admin.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 1 {
				t.Fatalf("pruning changed %s rows: %d", table, count)
			}
		}
		if _, err := resolver.AuthenticateKey(context.Background(), key.APIKey); err != nil {
			t.Fatal("pruning invalidated independent key")
		}
	})
	t.Run("CanonicalDatabaseClockOrderingMaximumRepeatedPasses", func(t *testing.T) {
		admin, _, resolver := prunerFixture(t)
		now := dbClock(t, admin)
		expiry := now.Add(-48 * time.Hour)
		// Expiry dominates ID; tied expiries use ID. Deliberately reverse the
		// first row's lexical position to distinguish both ordering keys.
		seedPruneToken(t, admin, "at_z_earliest", expiry.Add(-time.Hour), false)
		seedPruneToken(t, admin, "at_a_tied", expiry, false)
		seedPruneToken(t, admin, "at_b_tied", expiry, true)
		first, err := resolver.PruneTokens(context.Background(), 2)
		if err != nil {
			t.Fatal(err)
		}
		if first.DeletedCount != 2 || first.ExpiredBacklog != 1 || !first.OldestExpiry.Valid || !first.OldestExpiry.Time.Equal(expiry) {
			t.Fatalf("ordered pass=%+v", first)
		}
		if ids := pruneIDs(t, admin); !slices.Equal(ids, []string{"at_b_tied"}) {
			t.Fatalf("wrong expiry/ID selection: %v", ids)
		}
		if _, err := admin.Exec(`INSERT INTO auth_access_tokens(id,workspace_id,token_digest,federation_rule_id,identity_id,grant_id,rule_revision,identity_revision,grant_revision,role_version,created_at,expires_at)
 SELECT 'at_bulk_'||lpad(g::text,4,'0'),$1,decode(md5('prune-first-'||g)||md5('prune-second-'||g),'hex'),'rule_policy','identity_policy','grant_policy',1,1,1,$2,$3::timestamptz-interval '1 hour',$3 FROM generate_series(1,1001) g`, string(workspace.DefaultID), PolicyVersion, expiry); err != nil {
			t.Fatal(err)
		}
		seedPruneToken(t, admin, "at_recent", now.Add(-time.Hour), false)
		seedPruneToken(t, admin, "at_future", now.Add(time.Hour), false)
		zero, err := resolver.PruneTokens(context.Background(), -1)
		if err != nil {
			t.Fatal(err)
		}
		if zero.DeletedCount != 0 || zero.ExpiredBacklog != 1002 {
			t.Fatalf("negative batch=%+v", zero)
		}
		bounded, err := resolver.PruneTokens(context.Background(), 5000)
		if err != nil {
			t.Fatal(err)
		}
		if bounded.DeletedCount != 1000 || bounded.ExpiredBacklog != 2 {
			t.Fatalf("hard batch ceiling=%+v", bounded)
		}
		if ids := pruneIDs(t, admin); !slices.Equal(ids, []string{"at_bulk_1000", "at_bulk_1001", "at_future", "at_recent"}) {
			t.Fatalf("wrong maximum selection: %v", ids)
		}
		last, err := resolver.PruneTokens(context.Background(), 1000)
		if err != nil {
			t.Fatal(err)
		}
		if last.DeletedCount != 2 || last.ExpiredBacklog != 0 || last.OldestExpiry.Valid {
			t.Fatalf("repeated pass=%+v", last)
		}
		empty, err := resolver.PruneTokens(context.Background(), 1000)
		if err != nil {
			t.Fatal(err)
		}
		if empty.DeletedCount != 0 || empty.ExpiredBacklog != 0 || empty.OldestExpiry.Valid {
			t.Fatalf("empty pass=%+v", empty)
		}
		if ids := pruneIDs(t, admin); !slices.Equal(ids, []string{"at_future", "at_recent"}) {
			t.Fatalf("canonical clock deleted recent/future rows: %v", ids)
		}
	})
}

func TestAuthTokenPrunerConcurrentReplicas(t *testing.T) {
	admin, w, resolver := prunerFixture(t)
	expiry := dbClock(t, admin).Add(-48 * time.Hour)
	for _, label := range []string{"at_a", "at_b", "at_c"} {
		seedPruneToken(t, admin, label, expiry, false)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first, err := w.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Rollback() }()
	var a PruneResult
	if err := first.QueryRowContext(ctx, `SELECT deleted_count,expired_backlog,oldest_expiry FROM public.tetral_auth_prune_tokens(1)`).Scan(&a.DeletedCount, &a.ExpiredBacklog, &a.OldestExpiry); err != nil {
		t.Fatal(err)
	}
	if a.DeletedCount != 1 || a.ExpiredBacklog != 2 {
		t.Fatalf("first replica=%+v", a)
	}
	// The first deletion remains uncommitted and row-locked. The second
	// Auth connection must skip it rather than await the first transaction's commit.
	second := NewAuthorityResolver(w.OpenWorkload(t, "auth", nil), workspace.DefaultID)
	b, err := second.PruneTokens(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if b.DeletedCount != 1 {
		t.Fatalf("second replica deleted=%d", b.DeletedCount)
	}
	if ids := pruneIDs(t, admin); !slices.Equal(ids, []string{"at_a", "at_c"}) {
		t.Fatalf("second replica failed skip/order control: %v", ids)
	}
	if err := first.Commit(); err != nil {
		t.Fatal(err)
	}
	if ids := pruneIDs(t, admin); !slices.Equal(ids, []string{"at_c"}) {
		t.Fatalf("overlapping deletions=%v", ids)
	}
	final, err := resolver.PruneTokens(ctx, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if final.DeletedCount != 1 || final.ExpiredBacklog != 0 {
		t.Fatalf("remaining replica pass=%+v", final)
	}
}

func TestAuthTokenPrunerRoleIsolation(t *testing.T) {
	admin, w, resolver := prunerFixture(t)
	seedPruneToken(t, admin, "at_role_control", dbClock(t, admin).Add(-48*time.Hour), false)
	contract, err := database.LoadRoleContract()
	if err != nil {
		t.Fatal(err)
	}
	for _, role := range contract.WorkloadNames() {
		if role == "auth" {
			continue
		}
		t.Run(role, func(t *testing.T) {
			_, err := NewAuthorityResolver(w.OpenWorkload(t, role, nil), workspace.DefaultID).PruneTokens(context.Background(), 1000)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
				t.Fatalf("non-Auth role prune: want insufficient_privilege, got %T", err)
			}
		})
	}
	if _, err := w.DB.Exec(`DELETE FROM auth_access_tokens`); err == nil {
		t.Fatal("Auth bypassed narrow pruner with direct DELETE")
	}
	if ids := pruneIDs(t, admin); !slices.Equal(ids, []string{"at_role_control"}) {
		t.Fatal("role-denied pass changed rows")
	}
	result, err := resolver.PruneTokens(context.Background(), 1000)
	if err != nil {
		t.Fatal(err)
	}
	if result.DeletedCount != 1 {
		t.Fatal("Auth narrow deletion denied")
	}
}

type pruneQueryBarrier struct {
	match      string
	end        bool
	entered    chan struct{}
	release    chan struct{}
	once       sync.Once
	resumeOnce sync.Once
}

func (b *pruneQueryBarrier) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.HasPrefix(data.SQL, b.match) {
		if !b.end {
			b.pause(ctx)
		}
		return context.WithValue(ctx, pruneQueryBarrierKey{}, true)
	}
	return ctx
}

type pruneQueryBarrierKey struct{}

func (b *pruneQueryBarrier) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if b.end && ctx.Value(pruneQueryBarrierKey{}) == true {
		b.pause(ctx)
	}
}
func (b *pruneQueryBarrier) pause(ctx context.Context) {
	b.once.Do(func() {
		close(b.entered)
		select {
		case <-b.release:
		case <-ctx.Done():
		}
	})
}

func (b *pruneQueryBarrier) resume() {
	b.resumeOnce.Do(func() { close(b.release) })
}

func waitPruneSignal(ctx context.Context, t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatal("pruner barrier deadline")
	}
}

func testPruner(ctx context.Context, resolver *AuthorityResolver, logger *slog.Logger, ticks <-chan time.Time) *TokenPruner {
	ctx, cancel := context.WithCancel(ctx)
	p := &TokenPruner{cancel: cancel, done: make(chan struct{})}
	go func() { defer close(p.done); p.run(ctx, resolver.PruneTokens, logger, ticks) }()
	return p
}

type pruneBackendTracer struct{ started chan uint32 }

func (p *pruneBackendTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.HasPrefix(data.SQL, "SELECT deleted_count,expired_backlog,oldest_expiry FROM public.tetral_auth_prune_tokens") {
		select {
		case p.started <- conn.PgConn().PID():
		case <-ctx.Done():
		}
	}
	return ctx
}

func (*pruneBackendTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func pruneBackendState(ctx context.Context, t *testing.T, admin *sql.DB, pid uint32) string {
	t.Helper()
	var state, wait string
	err := admin.QueryRowContext(ctx, `SELECT state,COALESCE(wait_event_type,'') FROM pg_stat_activity WHERE datname=current_database() AND pid=$1`, pid).Scan(&state, &wait)
	if errors.Is(err, sql.ErrNoRows) {
		return "gone"
	}
	if err != nil {
		t.Fatal(err)
	}
	return state + "/" + wait
}

func TestAuthTokenPrunerAdmissionAndShutdown(t *testing.T) {
	t.Run("PurgedCredentialCannotCompleteStaleAdmission", func(t *testing.T) {
		admin, w, _ := prunerFixture(t)
		now := dbClock(t, admin)
		raw := seedPruneToken(t, admin, "at_admission", now.Add(time.Hour), false)
		// Hold after initial lookup/root checks but before the credential row
		// lock, so expiry update and physical purge can precede admission.
		barrier := &pruneQueryBarrier{match: "SELECT id FROM auth_access_tokens WHERE", entered: make(chan struct{}), release: make(chan struct{})}
		traced := storagetest.OpenRuntimeRoleDBWithTracer(t, w.DB, barrier)
		resolver := NewAuthorityResolver(traced, workspace.DefaultID)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		result := make(chan error, 1)
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, err := resolver.AuthenticateBearer(ctx, raw)
			result <- err
		}()
		defer func() {
			cancel()
			barrier.resume()
			joinCtx, joinCancel := context.WithTimeout(context.Background(), time.Second)
			defer joinCancel()
			waitPruneSignal(joinCtx, t, done)
		}()
		waitPruneSignal(ctx, t, barrier.entered)
		if _, err := admin.ExecContext(ctx, `UPDATE auth_access_tokens SET expires_at=$1 WHERE id='at_admission'`, now.Add(-48*time.Hour)); err != nil {
			t.Fatal(err)
		}
		pruned, err := NewAuthorityResolver(w.DB, workspace.DefaultID).PruneTokens(ctx, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if pruned.DeletedCount != 1 {
			t.Fatal("stale admission row was not physically pruned")
		}
		barrier.resume()
		select {
		case err := <-result:
			var rejected *AuthenticationError
			if !errors.As(err, &rejected) {
				t.Fatalf("purged stale admission result=%T", err)
			}
		case <-ctx.Done():
			t.Fatal("admission did not join")
		}
		if ids := pruneIDs(t, admin); len(ids) != 0 {
			t.Fatal("stale admission recreated token")
		}
	})
	t.Run("AdmittedLiveCredentialAndExpiredPruningProceed", func(t *testing.T) {
		admin, w, _ := prunerFixture(t)
		now := dbClock(t, admin)
		raw := seedPruneToken(t, admin, "at_live_admission", now.Add(time.Hour), false)
		seedPruneToken(t, admin, "at_other_expired", now.Add(-48*time.Hour), false)
		barrier := &pruneQueryBarrier{match: "UPDATE auth_access_tokens SET last_used_at", end: true, entered: make(chan struct{}), release: make(chan struct{})}
		resolver := NewAuthorityResolver(storagetest.OpenRuntimeRoleDBWithTracer(t, w.DB, barrier), workspace.DefaultID)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		result := make(chan error, 1)
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, err := resolver.AuthenticateBearer(ctx, raw)
			result <- err
		}()
		defer func() {
			cancel()
			barrier.resume()
			joinCtx, joinCancel := context.WithTimeout(context.Background(), time.Second)
			defer joinCancel()
			waitPruneSignal(joinCtx, t, done)
		}()
		waitPruneSignal(ctx, t, barrier.entered)
		pruned, err := NewAuthorityResolver(w.DB, workspace.DefaultID).PruneTokens(ctx, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if pruned.DeletedCount != 1 {
			t.Fatal("live admission blocked expired pruning")
		}
		barrier.resume()
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("admitted live credential failed: %T", err)
			}
		case <-ctx.Done():
			t.Fatal("live admission did not join")
		}
		var touched bool
		if err := admin.QueryRow(`SELECT last_used_at IS NOT NULL FROM auth_access_tokens WHERE id='at_live_admission'`).Scan(&touched); err != nil {
			t.Fatal(err)
		}
		if !touched {
			t.Fatal("live admission did not commit usage")
		}
	})
	t.Run("DatabaseDeadlineAndJoinedCancellation", func(t *testing.T) {
		admin, w, _ := prunerFixture(t)
		tracer := &pruneBackendTracer{started: make(chan uint32, 4)}
		resolver := NewAuthorityResolver(storagetest.OpenRuntimeRoleDBWithTracer(t, w.DB, tracer), workspace.DefaultID)
		seedPruneToken(t, admin, "at_blocked", dbClock(t, admin).Add(-48*time.Hour), false)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		lock, err := admin.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = lock.Rollback() }()
		if _, err := lock.ExecContext(ctx, `LOCK TABLE auth_access_tokens IN ACCESS EXCLUSIVE MODE`); err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		_, err = resolver.PruneTokens(ctx, 1000)
		if !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil || time.Since(started) > 5*time.Second {
			t.Fatal("blocked canonical pruner exceeded its two-second database deadline")
		}
		deadlinePID := <-tracer.started
		ticks := make(chan time.Time)
		p := testPruner(ctx, resolver, nil, ticks)
		defer p.Close()
		ticks <- time.Now()
		var maintenancePID uint32
		select {
		case maintenancePID = <-tracer.started:
		case <-ctx.Done():
			t.Fatal("maintenance query did not start")
		}
		// Observe the actual lock-wait in PostgreSQL, not a guessed delay.
		for {
			var waiting bool
			if err := admin.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid=$1 AND query LIKE 'SELECT deleted_count,expired_backlog,oldest_expiry FROM public.tetral_auth_prune_tokens%' AND state='active' AND wait_event_type='Lock')`, maintenancePID).Scan(&waiting); err != nil {
				t.Fatal(err)
			}
			if waiting {
				break
			}
			if ctx.Err() != nil {
				t.Fatal("maintenance query never reached database lock barrier")
			}
		}
		closed := make(chan struct{})
		go func() { p.Close(); close(closed) }()
		waitPruneSignal(ctx, t, closed)
		select {
		case <-p.done:
		default:
			t.Fatal("Close returned before maintenance joined")
		}
		if err := lock.Rollback(); err != nil {
			t.Fatal(err)
		}
		if ids := pruneIDs(t, admin); !slices.Equal(ids, []string{"at_blocked"}) {
			t.Fatalf("cancelled pass partially deleted rows: remaining=%v; deadline backend pid=%d state=%s; maintenance backend pid=%d state=%s", ids, deadlinePID, pruneBackendState(ctx, t, admin, deadlinePID), maintenancePID, pruneBackendState(ctx, t, admin, maintenancePID))
		}
		result, err := resolver.PruneTokens(ctx, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if result.DeletedCount != 1 {
			t.Fatalf("later pass could not reclaim cancelled work: result=%+v; deadline backend pid=%d state=%s; maintenance backend pid=%d state=%s", result, deadlinePID, pruneBackendState(ctx, t, admin, deadlinePID), maintenancePID, pruneBackendState(ctx, t, admin, maintenancePID))
		}
	})
}

func TestTokenPrunerFailureRecoveryAndMetrics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ticks := make(chan time.Time)
	entered := make(chan int)
	release := make(chan struct{})
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	p := &TokenPruner{}
	pass := 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.run(ctx, func(ctx context.Context, batch int) (PruneResult, error) {
			pass++
			select {
			case entered <- batch:
			case <-ctx.Done():
				return PruneResult{}, ctx.Err()
			}
			select {
			case <-release:
			case <-ctx.Done():
				return PruneResult{}, ctx.Err()
			}
			if pass == 2 {
				return PruneResult{}, errors.New("secret sentinel\r\nforged oversized " + strings.Repeat("x", 4096))
			}
			return PruneResult{DeletedCount: 2, ExpiredBacklog: 7, OldestExpiry: sql.NullTime{Time: time.Now().Add(-48 * time.Hour), Valid: true}}, nil
		}, logger, ticks)
	}()
	// Collection races with passes by design; its fixed label vocabulary cannot
	// grow with a token, workspace or error. The race detector checks ownership.
	scrapesDone := make(chan struct{})
	go func() {
		defer close(scrapesDone)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				_, _ = p.Collector()(ctx)
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		joinCtx, joinCancel := context.WithTimeout(context.Background(), time.Second)
		defer joinCancel()
		waitPruneSignal(joinCtx, t, done)
		waitPruneSignal(joinCtx, t, scrapesDone)
	})
	for n := 1; n <= 3; n++ {
		select {
		case ticks <- time.Now():
		case <-ctx.Done():
			t.Fatal("tick delivery deadline")
		}
		select {
		case batch := <-entered:
			if batch != 1000 {
				t.Fatalf("production pass batch=%d", batch)
			}
		case <-ctx.Done():
			t.Fatal("pass missing")
		}
		if n == 3 {
			observed, err := p.Collector()(ctx)
			if err != nil {
				t.Fatal(err)
			}
			values := map[string]float64{}
			for _, metric := range observed {
				values[metric.Name] = metric.Value
			}
			if values["tetral_auth_token_prune_expired_backlog"] != 7 || values["tetral_auth_token_prune_healthy"] != 0 || values["tetral_auth_token_prune_last_success_timestamp_seconds"] == 0 {
				t.Fatal("failed pass discarded last successful capacity observation or remained healthy")
			}
		}
		select {
		case release <- struct{}{}:
		case <-ctx.Done():
			t.Fatal("pass release deadline")
		}
		// The next receive proves the preceding pass completed its result update.
		if n < 3 {
			continue
		}
	}
	close(ticks)
	waitPruneSignal(ctx, t, done)
	cancel()
	joinCtx, joinCancel := context.WithTimeout(context.Background(), time.Second)
	defer joinCancel()
	waitPruneSignal(joinCtx, t, scrapesDone)
	metrics, err := p.Collector()(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]float64{}
	for _, metric := range metrics {
		key := metric.Name
		for _, label := range metric.Labels {
			if label.Name != "status" || !slices.Contains([]string{"success", "failed", "cancelled"}, label.Value) {
				t.Fatalf("unbounded metric label=%+v", label)
			}
			key += "/" + label.Value
		}
		values[key] = metric.Value
	}
	for name, want := range map[string]float64{"tetral_auth_token_prune_passes_total/success": 2, "tetral_auth_token_prune_passes_total/failed": 1, "tetral_auth_token_prune_passes_total/cancelled": 0, "tetral_auth_token_prune_deleted_total": 4, "tetral_auth_token_prune_expired_backlog": 7, "tetral_auth_token_prune_healthy": 1} {
		if values[name] != want {
			t.Fatalf("metric %s=%g want=%g", name, values[name], want)
		}
	}
	if len(metrics) != 8 || values["tetral_auth_token_prune_oldest_expiry_age_seconds"] < 48*time.Hour.Seconds() || values["tetral_auth_token_prune_last_success_timestamp_seconds"] == 0 {
		t.Fatal("missing bounded last-success capacity snapshot")
	}
	decoder := json.NewDecoder(&logs)
	var records []map[string]any
	for decoder.More() {
		var record map[string]any
		if err := decoder.Decode(&record); err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	if len(records) != 2 || records[0]["msg"] != "auth.token_prune.failed" || records[0]["error.class"] != "dependency_unavailable" || records[1]["recovery.event"] != "auth.token_prune.failed" {
		t.Fatal("failure/recovery diagnostics lack bounded classification")
	}
	for _, record := range records {
		encoded, _ := json.Marshal(record)
		if bytes.Contains(encoded, []byte("sentinel")) || len(encoded) > 1024 {
			t.Fatal("dependency error leaked or made diagnostic unbounded")
		}
	}
}
