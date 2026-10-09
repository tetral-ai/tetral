package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Trace: a future-only tenant yields after one 32-row window, a ready tenant
// leases within two turns, and later turns resume the retained cursor.
func TestJobRunnerLeaseFutureTenantYieldsAndResumesItsWindow(t *testing.T) {
	store, admin, _ := newJobRunnerTestStore(t, nil)
	now := time.Now().UTC()
	insertJobRunnerSeries(t, admin, "ws_jr_trace_a", 100, 100, now.Add(time.Hour))
	insertJobRunnerRows(t, admin, jobRunnerRow{ID: "qjob_trace_b", Workspace: "ws_jr_trace_b", Kind: KindRuntimeInput, Session: "sesn_trace_b", Thread: "thrd_trace_b", Sequence: 1, AvailableAt: now.Add(-time.Minute)})

	first := leaseJobRunner(t, store, 1)
	assertJobIDs(t, first.Jobs, "qjob_trace_b")
	if pass := jobRunnerPassSnapshot(store, "ws_jr_trace_a"); pass == nil || pass.examined != 32 {
		t.Fatalf("future tenant pass after first call = %+v; want 32 examined rows", pass)
	}
	for _, want := range []int{64, 96} {
		if got := leaseJobRunner(t, store, 1); len(got.Jobs) != 0 {
			t.Fatalf("future-only tenant leased %v", got.Jobs)
		}
		if pass := jobRunnerPassSnapshot(store, "ws_jr_trace_a"); pass == nil || pass.examined != want {
			t.Fatalf("future tenant pass = %+v; want %d examined rows without restarting", pass, want)
		}
	}
	if got := leaseJobRunner(t, store, 1); len(got.Jobs) != 0 {
		t.Fatalf("future-only tenant leased %v", got.Jobs)
	}
	if pass := jobRunnerPassSnapshot(store, "ws_jr_trace_a"); pass != nil {
		t.Fatalf("pass after its last 4 rows = %+v; want completed and removed", pass)
	}

	if _, err := admin.Exec(`UPDATE queue_jobs SET available_at = clock_timestamp() - interval '1 second' WHERE workspace_id = 'ws_jr_trace_a'`); err != nil {
		t.Fatal(err)
	}
	due := leaseJobRunner(t, store, 1)
	if len(due.Jobs) != 1 || due.Jobs[0].WorkspaceID != "ws_jr_trace_a" {
		t.Fatalf("new pass after A became due leased %v; want one A job", due.Jobs)
	}
}

// Trace: a job admitted after its tenant's pass began lies beyond that pass's
// upper key. The call that ends the retained pass returns no job and the
// discovery-complete hint; the next call starts a fresh pass and leases it.
func TestJobRunnerLeaseTakesWorkAdmittedAfterARetainedPassOnTheNextCall(t *testing.T) {
	store, admin, _ := newJobRunnerTestStore(t, nil)
	due := time.Now().UTC().Add(-time.Hour)
	insertJobRunnerRows(t, admin,
		jobRunnerRow{ID: "qjob_retained_j1", Workspace: "ws_jr_retained", Kind: KindRuntimeInput, Session: "sesn_retained_1", Thread: "thrd_retained", Sequence: 1, AvailableAt: due},
		jobRunnerRow{ID: "qjob_retained_j2", Workspace: "ws_jr_retained", Kind: KindRuntimeInput, Session: "sesn_retained_1", Thread: "thrd_retained", Sequence: 2, AvailableAt: due.Add(time.Second)},
	)
	first := leaseJobRunner(t, store, 1)
	assertJobIDs(t, first.Jobs, "qjob_retained_j1")
	if pass := jobRunnerPassSnapshot(store, "ws_jr_retained"); pass == nil || pass.after.id != "qjob_retained_j1" || pass.upper.id != "qjob_retained_j2" {
		t.Fatalf("pass after the first lease = %+v; want it retained after j1 with upper j2", pass)
	}

	insertJobRunnerRows(t, admin, jobRunnerRow{ID: "qjob_retained_j3", Workspace: "ws_jr_retained", Kind: KindRuntimeInput, Session: "sesn_retained_3", Thread: "thrd_retained", Sequence: 1, AvailableAt: time.Now().UTC().Add(-time.Second)})
	second := leaseJobRunner(t, store, 1)
	if len(second.Jobs) != 0 || second.RetryAfter != jobRunnerIdleRetryAfter {
		t.Fatalf("call ending the retained pass = %v/%s; want no job and the 1000 ms hint", second.Jobs, second.RetryAfter)
	}
	third := leaseJobRunner(t, store, 1)
	assertJobIDs(t, third.Jobs, "qjob_retained_j3")
}

// Trace: a tenant whose prefix is blocked in one lane crosses the prefix on
// later opportunities while another tenant keeps leasing; the lease
// transaction budget advances only the candidates actually examined.
func TestJobRunnerLeaseBlockedPrefixAdvancesOnlyExaminedCandidates(t *testing.T) {
	store, admin, _ := newJobRunnerTestStore(t, nil)
	now := time.Now().UTC()
	due := now.Add(-time.Hour)
	rows := []jobRunnerRow{{ID: "qjob_block_leased", Workspace: "ws_jr_block_a", Kind: KindRuntimeInput, Session: "sesn_block", Thread: "thrd_block_x", Sequence: 1, Status: StatusLeased, AvailableAt: due}}
	for index := 0; index < 65; index++ {
		rows = append(rows, jobRunnerRow{ID: fmt.Sprintf("qjob_block_x%02d", index), Workspace: "ws_jr_block_a", Kind: KindRuntimeInput, Session: "sesn_block", Thread: "thrd_block_x", Sequence: int64(index + 2), AvailableAt: due})
	}
	rows = append(rows, jobRunnerRow{ID: "qjob_block_y", Workspace: "ws_jr_block_a", Kind: KindRuntimeInput, Session: "sesn_block", Thread: "thrd_block_y", Sequence: 67, AvailableAt: due})
	for index := 0; index < 8; index++ {
		rows = append(rows, jobRunnerRow{ID: fmt.Sprintf("qjob_hot_b%d", index), Workspace: "ws_jr_block_b", Kind: KindRuntimeInput, Session: fmt.Sprintf("sesn_hot_%d", index), Thread: "thrd_hot", Sequence: 1, AvailableAt: due.Add(time.Duration(index) * time.Second)})
	}
	insertJobRunnerRows(t, admin, rows...)

	first := leaseJobRunner(t, store, 16)
	if len(first.Jobs) != 0 || first.RetryAfter != jobRunnerActiveRetryAfter {
		t.Fatalf("first call = %v/%s; want no lease and the active hint after its transaction budget", first.Jobs, first.RetryAfter)
	}
	if pass := jobRunnerPassSnapshot(store, "ws_jr_block_a"); pass == nil || pass.examined != 16 || pass.after.id != "qjob_block_x15" {
		t.Fatalf("blocked tenant after one budgeted turn = %+v; want exactly 16 examined", pass)
	}
	var leasedB, leasedY int
	for call := 0; call < 6 && leasedY == 0; call++ {
		result := leaseJobRunner(t, store, 16)
		for _, job := range result.Jobs {
			switch {
			case job.ID == "qjob_block_y":
				leasedY++
			case job.WorkspaceID == "ws_jr_block_b":
				leasedB++
			default:
				t.Fatalf("blocked follower %s was leased", job.ID)
			}
		}
	}
	if leasedY != 1 || leasedB < 4 {
		t.Fatalf("independent lane leases=%d, hot tenant leases=%d; want the lane reached while B interleaves", leasedY, leasedB)
	}
	var pendingX int
	if err := admin.QueryRow(`SELECT count(*) FROM queue_jobs WHERE id LIKE 'qjob_block_x%' AND status = 'pending'`).Scan(&pendingX); err != nil || pendingX != 65 {
		t.Fatalf("blocked lane pending rows = %d/%v; want all 65 still pending", pendingX, err)
	}
}

// Trace: one call never takes two jobs from a tenant. Wrapping to a visited
// tenant ends the call, including when another call moves the shared cursor
// between this call's seek and its cursor commit.
func TestJobRunnerLeaseVisitedTenantEndsCall(t *testing.T) {
	store, admin, _ := newJobRunnerTestStore(t, nil)
	due := time.Now().UTC().Add(-time.Hour)
	insertJobRunnerRows(t, admin,
		jobRunnerRow{ID: "qjob_visit_a1", Workspace: "ws_jr_visit_a", Kind: KindRuntimeInput, Session: "sesn_va1", Thread: "thrd_v", Sequence: 1, AvailableAt: due},
		jobRunnerRow{ID: "qjob_visit_b1", Workspace: "ws_jr_visit_b", Kind: KindRuntimeInput, Session: "sesn_vb1", Thread: "thrd_v", Sequence: 1, AvailableAt: due},
		jobRunnerRow{ID: "qjob_visit_b2", Workspace: "ws_jr_visit_b", Kind: KindRuntimeInput, Session: "sesn_vb2", Thread: "thrd_v", Sequence: 1, AvailableAt: due.Add(time.Second)},
		jobRunnerRow{ID: "qjob_visit_c1", Workspace: "ws_jr_visit_c", Kind: KindRuntimeInput, Session: "sesn_vc1", Thread: "thrd_v", Sequence: 1, AvailableAt: due},
		jobRunnerRow{ID: "qjob_visit_c2", Workspace: "ws_jr_visit_c", Kind: KindRuntimeInput, Session: "sesn_vc2", Thread: "thrd_v", Sequence: 1, AvailableAt: due.Add(time.Second)},
	)
	result := leaseJobRunner(t, store, 8)
	assertJobIDs(t, result.Jobs, "qjob_visit_a1", "qjob_visit_b1", "qjob_visit_c1")

	// Second fixture: another call commits a cursor move while the first call
	// is between its seek and its commit; the stale proposal is discarded.
	store, admin, _ = newJobRunnerTestStore(t, nil)
	insertJobRunnerRows(t, admin,
		jobRunnerRow{ID: "qjob_visit_a1", Workspace: "ws_jr_visit_a", Kind: KindRuntimeInput, Session: "sesn_va1", Thread: "thrd_v", Sequence: 1, AvailableAt: due},
		jobRunnerRow{ID: "qjob_visit_b1", Workspace: "ws_jr_visit_b", Kind: KindRuntimeInput, Session: "sesn_vb1", Thread: "thrd_v", Sequence: 1, AvailableAt: due},
		jobRunnerRow{ID: "qjob_visit_b2", Workspace: "ws_jr_visit_b", Kind: KindRuntimeInput, Session: "sesn_vb2", Thread: "thrd_v", Sequence: 1, AvailableAt: due.Add(time.Second)},
		jobRunnerRow{ID: "qjob_visit_c1", Workspace: "ws_jr_visit_c", Kind: KindRuntimeInput, Session: "sesn_vc1", Thread: "thrd_v", Sequence: 1, AvailableAt: due},
		jobRunnerRow{ID: "qjob_visit_c2", Workspace: "ws_jr_visit_c", Kind: KindRuntimeInput, Session: "sesn_vc2", Thread: "thrd_v", Sequence: 1, AvailableAt: due.Add(time.Second)},
	)
	var interleaved LeaseJobRunnerJobsResult
	seeks := 0
	store.jobRunner.afterTenantSeek = func(string) {
		seeks++
		if seeks != 2 {
			return
		}
		interleaved = leaseJobRunner(t, store, 1)
	}
	outer := leaseJobRunner(t, store, 8)
	store.jobRunner.afterTenantSeek = nil
	assertJobIDs(t, interleaved.Jobs, "qjob_visit_b1")
	assertJobIDs(t, outer.Jobs, "qjob_visit_a1", "qjob_visit_c1", "qjob_visit_b2")
}

// Trace: future rows cost no lease transaction, so with five tenants of 40
// future rows the 128-row budget ends the call after four full 32-row
// windows, before the turn or transaction caps. The fifth tenant's window is
// never fetched, and the next call reaches it first.
func TestJobRunnerLeaseRawCandidateBudgetStopsTheCall(t *testing.T) {
	store, admin, _ := newJobRunnerTestStore(t, nil)
	future := time.Now().UTC().Add(time.Hour)
	tenants := []string{"ws_jr_rows_1", "ws_jr_rows_2", "ws_jr_rows_3", "ws_jr_rows_4", "ws_jr_rows_5"}
	for _, tenant := range tenants {
		insertJobRunnerSeries(t, admin, tenant, 40, 0, future)
	}
	var seeks []string
	store.jobRunner.afterTenantSeek = func(proposed string) { seeks = append(seeks, proposed) }

	first := leaseJobRunner(t, store, 16)
	if len(first.Jobs) != 0 || first.RetryAfter != jobRunnerActiveRetryAfter {
		t.Fatalf("first call = %v/%s; want no lease and the active hint after its row budget", first.Jobs, first.RetryAfter)
	}
	if want := tenants[:4]; !reflect.DeepEqual(seeks, want) {
		t.Fatalf("first call took turns %v; want %v", seeks, want)
	}
	fetched := 0
	for _, tenant := range tenants[:4] {
		pass := jobRunnerPassSnapshot(store, tenant)
		if pass == nil || pass.examined != 32 || pass.after.id != "qjob_"+tenant+"_032" {
			t.Fatalf("tenant %s pass = %+v; want its first 32-row window examined", tenant, pass)
		}
		fetched += pass.examined
	}
	if pass := jobRunnerPassSnapshot(store, tenants[4]); fetched != 128 || pass != nil {
		t.Fatalf("first call fetched %d rows and left fifth tenant pass %+v; want 128 rows and no fifth window", fetched, pass)
	}

	seeks = nil
	leaseJobRunner(t, store, 16)
	store.jobRunner.afterTenantSeek = nil
	if len(seeks) == 0 || seeks[0] != tenants[4] {
		t.Fatalf("next call took turns %v; want the fifth tenant first", seeks)
	}
	if pass := jobRunnerPassSnapshot(store, tenants[4]); pass == nil || pass.examined != 32 {
		t.Fatalf("fifth tenant pass after the next call = %+v; want its first 32-row window examined", pass)
	}
}

// Trace: seventeen tenants with one future row each cost one turn and one row
// apiece and no lease transaction, so the 16-turn budget ends the call before
// the seventeenth tenant; the next call starts there.
func TestJobRunnerLeaseTurnBudgetStopsTheCall(t *testing.T) {
	store, admin, _ := newJobRunnerTestStore(t, nil)
	future := time.Now().UTC().Add(time.Hour)
	var tenants []string
	for index := 1; index <= 17; index++ {
		tenant := fmt.Sprintf("ws_jr_turns_%02d", index)
		tenants = append(tenants, tenant)
		insertJobRunnerSeries(t, admin, tenant, 1, 0, future)
	}
	var seeks []string
	store.jobRunner.afterTenantSeek = func(proposed string) { seeks = append(seeks, proposed) }

	first := leaseJobRunner(t, store, 16)
	if len(first.Jobs) != 0 || first.RetryAfter != jobRunnerActiveRetryAfter {
		t.Fatalf("first call = %v/%s; want no lease and the active hint after its turn budget", first.Jobs, first.RetryAfter)
	}
	if want := tenants[:16]; !reflect.DeepEqual(seeks, want) {
		t.Fatalf("first call took %d turns %v; want exactly the first 16 tenants", len(seeks), seeks)
	}

	seeks = nil
	leaseJobRunner(t, store, 16)
	store.jobRunner.afterTenantSeek = nil
	if len(seeks) == 0 || seeks[0] != tenants[16] {
		t.Fatalf("next call took turns %v; want the seventeenth tenant first", seeks)
	}
}

// Trace: one process alternates tenants across repeated single-job calls;
// replicas keep separate cursors but exact row custody admits one winner.
func TestJobRunnerLeaseAlternatesPerProcessAndReplicasShareExactCustody(t *testing.T) {
	store, admin, workload := newJobRunnerTestStore(t, nil)
	due := time.Now().UTC().Add(-time.Hour)
	var rows []jobRunnerRow
	for index := 0; index < 4; index++ {
		for _, tenant := range []string{"a", "b"} {
			rows = append(rows, jobRunnerRow{ID: fmt.Sprintf("qjob_alt_%s%d", tenant, index), Workspace: "ws_jr_alt_" + tenant, Kind: KindRuntimeInput, Session: fmt.Sprintf("sesn_alt_%s%d", tenant, index), Thread: "thrd_alt", Sequence: 1, AvailableAt: due.Add(time.Duration(index) * time.Second)})
		}
	}
	insertJobRunnerRows(t, admin, rows...)
	var tenants []string
	for range 4 {
		result := leaseJobRunner(t, store, 1)
		if len(result.Jobs) != 1 {
			t.Fatalf("single-job call returned %v", result.Jobs)
		}
		tenants = append(tenants, result.Jobs[0].WorkspaceID.String())
	}
	if want := []string{"ws_jr_alt_a", "ws_jr_alt_b", "ws_jr_alt_a", "ws_jr_alt_b"}; !reflect.DeepEqual(tenants, want) {
		t.Fatalf("one process reserved %v; want %v", tenants, want)
	}

	// Two replicas: replica one pauses just before its exact lease of a2 while
	// replica two, starting from its own cursor, takes the same candidate.
	paused, release := make(chan struct{}), make(chan struct{})
	var pauseOnce sync.Once
	tracer := &jobRunnerTrace{onStart: func(ctx context.Context, sql string) {
		if strings.Contains(sql, "pg_try_advisory_xact_lock") {
			pauseOnce.Do(func() {
				close(paused)
				select {
				case <-release:
				case <-ctx.Done():
				}
			})
		}
	}}
	replicaOne := NewPostgreSQLStore(dbconnect.NewClientForTesting(workload.OpenWorkload(t, "queue", tracer)))
	replicaTwo := NewPostgreSQLStore(dbconnect.NewClientForTesting(workload.OpenWorkload(t, "queue", nil)))
	first := make(chan LeaseJobRunnerJobsResult, 1)
	go func() {
		result, err := replicaOne.LeaseJobRunnerJobs(context.Background(), LeaseJobRunnerJobsRequest{LeaseOwner: "replica-one", MaxJobs: 1, LeaseDuration: time.Minute})
		if err != nil {
			t.Errorf("replica one: %v", err)
		}
		first <- result
	}()
	<-paused
	second := leaseJobRunner(t, replicaTwo, 1)
	close(release)
	firstResult := <-first
	assertJobIDs(t, second.Jobs, "qjob_alt_a2")
	if len(firstResult.Jobs) != 1 || firstResult.Jobs[0].ID == "qjob_alt_a2" || firstResult.Jobs[0].WorkspaceID != "ws_jr_alt_a" {
		t.Fatalf("paused replica leased %v; want another A job after losing a2", firstResult.Jobs)
	}
	var progressB bool
	for range 2 {
		for _, replica := range []*PostgreSQLQueueStore{replicaOne, replicaTwo} {
			for _, job := range leaseJobRunner(t, replica, 1).Jobs {
				progressB = progressB || job.WorkspaceID == "ws_jr_alt_b"
			}
		}
	}
	if !progressB {
		t.Fatal("replicas never leased tenant B")
	}
}

// Trace: a failure after one committed lease returns that lease with the
// active hint; a failure before any commit is the call's error. A lease whose
// response is lost is rediscovered only after the existing reclaim.
func TestJobRunnerLeaseFailureAfterCommitReturnsCommittedTokens(t *testing.T) {
	store, admin, _ := newJobRunnerTestStore(t, nil)
	due := time.Now().UTC().Add(-time.Hour)
	insertJobRunnerRows(t, admin,
		jobRunnerRow{ID: "qjob_fault_first", Workspace: "ws_jr_fault_a", Kind: KindRuntimeInput, Session: "sesn_fault_a", Thread: "thrd_fault", Sequence: 1, AvailableAt: due},
		jobRunnerRow{ID: "qjob_fault_marker", Workspace: "ws_jr_fault_b", Kind: KindRuntimeInput, Session: "sesn_fault_b", Thread: "thrd_fault", Sequence: 1, AvailableAt: due},
	)
	if _, err := admin.Exec(`CREATE FUNCTION queue_test_fail_marker_lease() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'fixture connection failure' USING ERRCODE = '08006'; END $$;
		CREATE TRIGGER queue_test_fail_marker_lease BEFORE UPDATE ON queue_jobs
		FOR EACH ROW WHEN (OLD.id = 'qjob_fault_marker' AND NEW.status = 'leased')
		EXECUTE FUNCTION queue_test_fail_marker_lease()`); err != nil {
		t.Fatal(err)
	}
	partial := leaseJobRunner(t, store, 2)
	assertJobIDs(t, partial.Jobs, "qjob_fault_first")
	var pgErr *pgconn.PgError
	if partial.RetryAfter != jobRunnerActiveRetryAfter || !errors.As(partial.StopFailure, &pgErr) || pgErr.Code != "08006" {
		t.Fatalf("partial result hint=%s failure=%v; want committed token, 100 ms and the stopping failure", partial.RetryAfter, partial.StopFailure)
	}
	if _, err := store.LeaseJobRunnerJobs(context.Background(), LeaseJobRunnerJobsRequest{LeaseOwner: "fault", MaxJobs: 2, LeaseDuration: time.Minute}); !errors.As(err, &pgErr) {
		t.Fatalf("failure before any commit = %v; want the call error", err)
	}
	if _, err := admin.Exec(`DROP TRIGGER queue_test_fail_marker_lease ON queue_jobs`); err != nil {
		t.Fatal(err)
	}

	lost := leaseJobRunner(t, store, 1)
	assertJobIDs(t, lost.Jobs, "qjob_fault_marker")
	if again := leaseJobRunner(t, store, 1); len(again.Jobs) != 0 {
		t.Fatalf("live lease with a lost response was rediscovered: %v", again.Jobs)
	}
	expireQueueJobLease(t, admin, "ws_jr_fault_b", "qjob_fault_marker")
	if reclaimed, err := store.ReclaimExpiredLeases(context.Background(), ReclaimExpiredLeasesRequest{}); err != nil || reclaimed != 1 {
		t.Fatalf("reclaim = %d/%v; want the expired lease", reclaimed, err)
	}
	recovered := leaseJobRunner(t, store, 1)
	if len(recovered.Jobs) != 1 || recovered.Jobs[0].LeaseToken == lost.Jobs[0].LeaseToken {
		t.Fatalf("reclaimed job lease = %v; want a fresh token", recovered.Jobs)
	}
}

// The direct lease and workspace-scoped Lease evaluate one eligibility
// predicate. Each case runs Lease limited to the candidate kind, so blockers of
// other kinds are not returned by that call, then the exact direct lease on an
// identical fixture in other Sessions.
func TestJobRunnerExactLeaseMatchesWorkspaceLeaseEligibility(t *testing.T) {
	type blocker struct {
		kind, thread, control, status string
		sequence                      int64
		future                        bool
	}
	type eligibilityCase struct {
		name             string
		kind, control    string
		session          string // active, archived, deleted, terminated or missing
		sequence         int64
		future           bool
		attempts, max    int
		blockers         []blocker
		wantLeased       bool
		wantAttemptAfter int
	}
	cases := []eligibilityCase{
		{name: "due ordinary input", kind: KindRuntimeInput, session: "active", sequence: 5, wantLeased: true, wantAttemptAfter: 1},
		{name: "future candidate", kind: KindRuntimeInput, session: "active", sequence: 5, future: true},
		{name: "missing Session admits ordinary work", kind: KindRuntimeInput, session: "missing", sequence: 5, wantLeased: true, wantAttemptAfter: 1},
		{name: "deleted Session rejects ordinary work", kind: KindRuntimeInput, session: "deleted", sequence: 5},
		{name: "terminated Session rejects cleanup", kind: KindCleanupSession, session: "terminated", sequence: 5},
		{name: "archived Session admits config", kind: KindRuntimeConfigUpdate, session: "archived", sequence: 5, wantLeased: true, wantAttemptAfter: 1},
		{name: "delete cleanup requires deleted Session", kind: KindSessionDeleteCleanup, session: "deleted", sequence: 5, wantLeased: true, wantAttemptAfter: 1},
		{name: "delete cleanup rejects missing Session", kind: KindSessionDeleteCleanup, session: "missing", sequence: 5},
		{name: "delete cleanup rejects active Session", kind: KindSessionDeleteCleanup, session: "active", sequence: 5},
		{name: "leased config blocks ordinary Thread work", kind: KindRuntimeInput, session: "active", sequence: 5, blockers: []blocker{{kind: KindRuntimeConfigUpdate, status: StatusLeased, sequence: 1}}},
		{name: "leased config does not block interrupt", kind: KindRuntimeInput, control: ControlClassInterrupt, session: "active", sequence: 5, blockers: []blocker{{kind: KindRuntimeConfigUpdate, status: StatusLeased, sequence: 1}}, wantLeased: true, wantAttemptAfter: 1},
		{name: "leased same-Thread recovery blocks input", kind: KindRuntimeInput, session: "active", sequence: 5, blockers: []blocker{{kind: KindRuntimeRecovery, status: StatusLeased, sequence: 1}}},
		{name: "leased other-Thread input does not block", kind: KindRuntimeInput, session: "active", sequence: 5, blockers: []blocker{{kind: KindRuntimeInput, thread: "other", status: StatusLeased, sequence: 1}}, wantLeased: true, wantAttemptAfter: 1},
		{name: "leased Thread work blocks Session cleanup", kind: KindCleanupSession, session: "active", sequence: 5, blockers: []blocker{{kind: KindRuntimeInput, thread: "other", status: StatusLeased, sequence: 1}}},
		{name: "leased config blocks Session cleanup", kind: KindCleanupSession, session: "active", sequence: 5, blockers: []blocker{{kind: KindRuntimeConfigUpdate, status: StatusLeased, sequence: 1}}},
		{name: "due earlier input blocks Session cleanup", kind: KindCleanupSession, session: "active", sequence: 5, blockers: []blocker{{kind: KindRuntimeInput, sequence: 1}}},
		{name: "future earlier input does not block Session cleanup", kind: KindCleanupSession, session: "active", sequence: 5, blockers: []blocker{{kind: KindRuntimeInput, sequence: 1, future: true}}, wantLeased: true, wantAttemptAfter: 1},
		{name: "delete cleanup ignores other pending kinds", kind: KindSessionDeleteCleanup, session: "deleted", sequence: 5, blockers: []blocker{{kind: KindRuntimeInput, sequence: 1}}, wantLeased: true, wantAttemptAfter: 1},
		{name: "future earlier config blocks ordinary input", kind: KindRuntimeInput, session: "active", sequence: 5, blockers: []blocker{{kind: KindRuntimeConfigUpdate, sequence: 1, future: true}}},
		{name: "earlier config does not block interrupt", kind: KindRuntimeInput, control: ControlClassInterrupt, session: "active", sequence: 5, blockers: []blocker{{kind: KindRuntimeConfigUpdate, sequence: 1}}, wantLeased: true, wantAttemptAfter: 1},
		{name: "pending recovery blocks interrupt", kind: KindRuntimeInput, control: ControlClassInterrupt, session: "active", sequence: 5, blockers: []blocker{{kind: KindRuntimeRecovery, sequence: 9, future: true}}},
		{name: "later pending interrupt blocks ordinary input", kind: KindRuntimeInput, session: "active", sequence: 5, blockers: []blocker{{kind: KindRuntimeInput, control: ControlClassInterrupt, sequence: 9}}},
		{name: "earlier interrupt blocks later interrupt", kind: KindRuntimeInput, control: ControlClassInterrupt, session: "active", sequence: 5, blockers: []blocker{{kind: KindRuntimeInput, control: ControlClassInterrupt, sequence: 1}}},
		{name: "future earlier ordinary blocks ordinary", kind: KindRuntimeInput, session: "active", sequence: 5, blockers: []blocker{{kind: KindRuntimeInput, sequence: 1, future: true}}},
		{name: "recovery ignores pending interrupt", kind: KindRuntimeRecovery, session: "active", sequence: 5, blockers: []blocker{{kind: KindRuntimeInput, control: ControlClassInterrupt, sequence: 1}}, wantLeased: true, wantAttemptAfter: 1},
		{name: "final interrupt attempt is clamped", kind: KindRuntimeInput, control: ControlClassInterrupt, session: "active", sequence: 5, attempts: 3, max: 3, wantLeased: true, wantAttemptAfter: 3},
		{name: "final cleanup attempt is clamped", kind: KindCleanupSession, session: "active", sequence: 5, attempts: 3, max: 3, wantLeased: true, wantAttemptAfter: 3},
		{name: "agent-mail finalization is clamped", kind: KindRuntimeInput, control: ControlClassAgentMail, session: "active", sequence: 5, attempts: 4, max: 3, wantLeased: true, wantAttemptAfter: 4},
		{name: "agent-mail final attempt increments", kind: KindRuntimeInput, control: ControlClassAgentMail, session: "active", sequence: 5, attempts: 3, max: 3, wantLeased: true, wantAttemptAfter: 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, admin, _ := newJobRunnerTestStore(t, nil)
			ws := workspace.DefaultID
			now := time.Now().UTC()
			build := func(variant string) string {
				sessionID := "sesn_matrix_" + variant
				if tc.session != "missing" {
					lifecycle, status := "active", "idle"
					switch tc.session {
					case "archived", "deleted":
						lifecycle = tc.session
					case "terminated":
						status = "terminated"
					}
					seedJobRunnerSession(t, admin, sessionID, lifecycle, status)
				}
				var rows []jobRunnerRow
				for index, b := range tc.blockers {
					thread := "thrd_matrix"
					if b.thread != "" {
						thread = "thrd_matrix_" + b.thread
					}
					available := now.Add(-time.Hour)
					if b.future {
						available = now.Add(time.Hour)
					}
					rows = append(rows, jobRunnerRow{ID: fmt.Sprintf("qjob_m%s_b%d", variant, index), Workspace: ws.String(), Kind: b.kind, Session: sessionID, Thread: thread, Control: b.control, Status: b.status, Sequence: b.sequence, AvailableAt: available})
				}
				available := now.Add(-time.Hour)
				if tc.future {
					available = now.Add(time.Hour)
				}
				candidateID := "qjob_m" + variant + "_candidate"
				rows = append(rows, jobRunnerRow{ID: candidateID, Workspace: ws.String(), Kind: tc.kind, Session: sessionID, Thread: "thrd_matrix", Control: tc.control, Sequence: tc.sequence, AvailableAt: available, AttemptCount: tc.attempts, MaxAttempts: tc.max})
				insertJobRunnerRows(t, admin, rows...)
				return candidateID
			}

			legacyID := build("l")
			leased, err := store.Lease(context.Background(), LeaseRequest{WorkspaceID: ws, Kinds: []string{tc.kind}, LeaseOwner: "matrix-legacy", MaxJobs: 16, LeaseDuration: time.Minute, Now: now})
			if err != nil {
				t.Fatalf("Lease: %v", err)
			}
			legacyLeased := false
			for _, job := range leased {
				legacyLeased = legacyLeased || job.ID == legacyID
			}

			directID := build("d")
			job, outcome, err := store.leaseJobRunnerCandidate(context.Background(), ws.String(),
				jobRunnerCandidate{key: jobRunnerKey{id: directID}, kind: tc.kind, causalSessionID: "sesn_matrix_d"},
				LeaseJobRunnerJobsRequest{LeaseOwner: "matrix-direct", MaxJobs: 1, LeaseDuration: time.Minute})
			if err != nil || (outcome != jobRunnerLeaseCommitted && outcome != jobRunnerLeaseIneligible) {
				t.Fatalf("direct exact lease outcome=%v err=%v; want a committed or ineligible decision", outcome, err)
			}
			directLeased := outcome == jobRunnerLeaseCommitted
			if legacyLeased != tc.wantLeased || directLeased != tc.wantLeased {
				t.Fatalf("leased legacy=%t direct=%t; want %t", legacyLeased, directLeased, tc.wantLeased)
			}
			if !tc.wantLeased {
				return
			}
			legacyAttempts, legacyPrevious := queueJobAttemptState(t, admin, legacyID)
			directAttempts, directPrevious := queueJobAttemptState(t, admin, directID)
			if legacyAttempts != tc.wantAttemptAfter || directAttempts != tc.wantAttemptAfter || job.AttemptCount != tc.wantAttemptAfter {
				t.Fatalf("attempts legacy=%d direct=%d; want %d", legacyAttempts, directAttempts, tc.wantAttemptAfter)
			}
			if legacyPrevious.Valid || !directPrevious.Valid || int(directPrevious.Int64) != tc.attempts {
				t.Fatalf("lease provenance legacy=%v direct=%v; want none for Lease and %d for the direct lease", legacyPrevious, directPrevious, tc.attempts)
			}
		})
	}
}

// Discovery statements, run under the Queue role exactly as the scheduler
// issues them, are served in order by the Job Runner partial indexes.
func TestJobRunnerDiscoveryStatementsUseJobRunnerIndexesUnderQueueRole(t *testing.T) {
	store, admin, _ := newJobRunnerTestStore(t, nil)
	now := time.Now().UTC()
	for _, tenant := range []string{"ws_jr_plan_a", "ws_jr_plan_b", "ws_jr_plan_c"} {
		insertJobRunnerSeries(t, admin, tenant, 64, 0, now.Add(-time.Hour))
	}
	insertJobRunnerSeries(t, admin, "ws_jr_plan_future", 8, 5, now.Add(time.Hour))
	if _, err := admin.Exec(`INSERT INTO queue_jobs (
		id, workspace_id, kind, partition_key, queue_partition_sequence, delivery_scope, control_class,
		payload_version, status, payload_json, priority, attempt_count, max_attempts, available_at, created_at, updated_at, acknowledged_at
	) SELECT 'qjob_plan_noise_' || value, 'ws_jr_plan_a', CASE WHEN value % 2 = 0 THEN 'environment_build' ELSE 'runtime_input' END,
	         'noise:' || value, 1, 'partition', 'ordinary', 1, CASE WHEN value % 2 = 0 THEN 'pending' ELSE 'acknowledged' END,
	         '{}', 0, 0, 10, $1::timestamptz, $1::timestamptz, $1::timestamptz, CASE WHEN value % 2 = 0 THEN NULL ELSE $1::timestamptz END
	    FROM generate_series(1, 2048) AS value`, now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(`ANALYZE queue_jobs`); err != nil {
		t.Fatal(err)
	}
	upper, _, found, err := store.jobRunnerStartPass(context.Background(), "ws_jr_plan_b")
	if err != nil || !found {
		t.Fatalf("pass start = %v/%v", found, err)
	}
	window, err := store.jobRunnerWindow(context.Background(), "ws_jr_plan_b", jobRunnerPass{started: true, upper: upper}, 32)
	if err != nil || len(window) != 32 {
		t.Fatalf("first window = %d/%v", len(window), err)
	}
	after := window[31].key
	upperArgs := []any{"ws_jr_plan_b", upper.negativePriority, upper.availableAt, upper.partitionKey, upper.partitionSequence, upper.id, 32}
	for _, statement := range []struct {
		name, index, sql string
		args             []any
		limit            int
	}{
		{"next tenant", "idx_queue_job_runner_scan", jobRunnerNextTenantSQL, []any{"ws_jr_plan_a"}, 1},
		{"first tenant", "idx_queue_job_runner_scan", jobRunnerFirstTenantSQL, nil, 1},
		{"pass upper", "idx_queue_job_runner_scan", jobRunnerPassUpperSQL, []any{"ws_jr_plan_b"}, 1},
		{"first window", "idx_queue_job_runner_scan", jobRunnerWindowSQL(false), upperArgs, 32},
		{"later window", "idx_queue_job_runner_scan", jobRunnerWindowSQL(true), append(append([]any{}, upperArgs...), after.negativePriority, after.availableAt, after.partitionKey, after.partitionSequence, after.id), 32},
		{"future hint", "idx_queue_job_runner_due", jobRunnerNextFutureSQL, nil, 1},
		{"pass cleanup", "idx_queue_job_runner_scan", jobRunnerAbsentTenantsSQL(3), []any{"ws_jr_plan_a", "ws_jr_plan_gone", "ws_jr_plan_c"}, 1},
	} {
		t.Run(statement.name, func(t *testing.T) {
			evidence := explainJobRunnerStatement(t, store, statement.sql, statement.args...)
			if !evidence.indexes[statement.index] || evidence.queueSeqScans != 0 || evidence.sorts != 0 {
				t.Fatalf("%s plan evidence = %+v\n%s", statement.name, evidence, evidence.raw)
			}
			if statement.limit > 0 && (evidence.maxQueueRows > float64(statement.limit) || evidence.maxQueueRowsRemoved != 0) {
				t.Fatalf("%s read beyond its bounded range: %+v\n%s", statement.name, evidence, evidence.raw)
			}
			// Window bounds must seek: each row comparison is an index condition.
			if bounds := strings.Count(statement.sql, ") <= (") + strings.Count(statement.sql, ") > ("); bounds > 0 {
				conds := strings.Join(evidence.indexConds, " ")
				if strings.Count(conds, "ROW(negative_priority") != bounds {
					t.Fatalf("%s index conditions %q do not carry its %d scan-key bounds\n%s", statement.name, conds, bounds, evidence.raw)
				}
			}
		})
	}
}

// The hint stays active after a budget stop while the process is busy, turns
// to the future-clamped value after a full cycle without a lease, and returns
// to 100 ms when a lease commits. Lock contention keeps an active process on
// the fast hint without waiting for the lock.
func TestJobRunnerLeaseRetryHintTracksIdleCycles(t *testing.T) {
	store, admin, _ := newJobRunnerTestStore(t, nil)
	due := time.Now().UTC().Add(-time.Hour)
	rows := []jobRunnerRow{{ID: "qjob_idle_leased", Workspace: "ws_jr_idle_a", Kind: KindRuntimeInput, Session: "sesn_idle", Thread: "thrd_idle", Sequence: 1, Status: StatusLeased, AvailableAt: due}}
	for index := 0; index < 40; index++ {
		rows = append(rows, jobRunnerRow{ID: fmt.Sprintf("qjob_idle_%02d", index), Workspace: "ws_jr_idle_a", Kind: KindRuntimeInput, Session: "sesn_idle", Thread: "thrd_idle", Sequence: int64(index + 2), AvailableAt: due})
	}
	insertJobRunnerRows(t, admin, rows...)

	if got := leaseJobRunner(t, store, 4); len(got.Jobs) != 0 || got.RetryAfter != jobRunnerActiveRetryAfter {
		t.Fatalf("first budgeted call = %v/%s; want the active hint", got.Jobs, got.RetryAfter)
	}
	if got := leaseJobRunner(t, store, 4); len(got.Jobs) != 0 || got.RetryAfter != jobRunnerIdleRetryAfter {
		t.Fatalf("call after a fruitless cycle = %v/%s; want the idle hint", got.Jobs, got.RetryAfter)
	}
	insertJobRunnerRows(t, admin, jobRunnerRow{ID: "qjob_idle_future", Workspace: "ws_jr_idle_c", Kind: KindRuntimeInput, Session: "sesn_idle_future", Thread: "thrd_idle", Sequence: 1, AvailableAt: time.Now().UTC().Add(600 * time.Millisecond)})
	if got := leaseJobRunner(t, store, 4); len(got.Jobs) != 0 || got.RetryAfter < jobRunnerActiveRetryAfter || got.RetryAfter > 600*time.Millisecond {
		t.Fatalf("idle call with a job due soon = %v/%s; want the clamped time until it", got.Jobs, got.RetryAfter)
	}
	if _, err := admin.Exec(`UPDATE queue_jobs SET available_at = clock_timestamp() + interval '1 hour' WHERE id = 'qjob_idle_future'`); err != nil {
		t.Fatal(err)
	}
	insertJobRunnerRows(t, admin, jobRunnerRow{ID: "qjob_idle_ready", Workspace: "ws_jr_idle_b", Kind: KindRuntimeInput, Session: "sesn_idle_ready", Thread: "thrd_idle", Sequence: 1, AvailableAt: due})
	if got := leaseJobRunner(t, store, 4); len(got.Jobs) != 1 || got.RetryAfter != jobRunnerActiveRetryAfter {
		t.Fatalf("call with a committed lease = %v/%s; want one lease and the active hint", got.Jobs, got.RetryAfter)
	}

	contended, admin2, _ := newJobRunnerTestStore(t, nil)
	insertJobRunnerRows(t, admin2, jobRunnerRow{ID: "qjob_contended", Workspace: "ws_jr_contended", Kind: KindRuntimeInput, Session: "sesn_contended", Thread: "thrd_contended", Sequence: 1, AvailableAt: due})
	holder, err := admin2.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback() }()
	if err := storage.AcquireSessionRuntimeMutationLock(context.Background(), holder, "ws_jr_contended", "sesn_contended"); err != nil {
		t.Fatal(err)
	}
	if got := leaseJobRunner(t, contended, 4); len(got.Jobs) != 0 || got.RetryAfter != jobRunnerActiveRetryAfter {
		t.Fatalf("call against a held Session owner = %v/%s; want no wait and the active hint", got.Jobs, got.RetryAfter)
	}
	if err := holder.Rollback(); err != nil {
		t.Fatal(err)
	}
	assertJobIDs(t, leaseJobRunner(t, contended, 4).Jobs, "qjob_contended")
}

// A tenant whose turn another call holds is skipped without being scanned or
// awaited; the cursor still advances past it.
func TestJobRunnerLeaseSkipsBusyTenantWithoutWaiting(t *testing.T) {
	paused, release := make(chan struct{}), make(chan struct{})
	var pauseOnce sync.Once
	tracer := &jobRunnerTrace{onStart: func(ctx context.Context, sql string) {
		if strings.Contains(sql, "pg_try_advisory_xact_lock") {
			pauseOnce.Do(func() {
				close(paused)
				select {
				case <-release:
				case <-ctx.Done():
				}
			})
		}
	}}
	store, admin, _ := newJobRunnerTestStore(t, tracer)
	insertJobRunnerRows(t, admin, jobRunnerRow{ID: "qjob_busy", Workspace: "ws_jr_busy", Kind: KindRuntimeInput, Session: "sesn_busy", Thread: "thrd_busy", Sequence: 1, AvailableAt: time.Now().UTC().Add(-time.Hour)})
	holder := make(chan LeaseJobRunnerJobsResult, 1)
	go func() {
		result, err := store.LeaseJobRunnerJobs(context.Background(), LeaseJobRunnerJobsRequest{LeaseOwner: "holder", MaxJobs: 1, LeaseDuration: time.Minute})
		if err != nil {
			t.Errorf("holder call: %v", err)
		}
		holder <- result
	}()
	<-paused
	windowsBefore := tracer.count("LIMIT $7")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	skipped, err := store.LeaseJobRunnerJobs(ctx, LeaseJobRunnerJobsRequest{LeaseOwner: "visitor", MaxJobs: 1, LeaseDuration: time.Minute})
	if err != nil || len(skipped.Jobs) != 0 {
		t.Fatalf("call meeting a busy tenant = %v/%v; want an immediate empty result", skipped.Jobs, err)
	}
	if windows := tracer.count("LIMIT $7") - windowsBefore; windows != 0 {
		t.Fatalf("busy tenant was scanned %d times by the visiting call", windows)
	}
	close(release)
	assertJobIDs(t, (<-holder).Jobs, "qjob_busy")
}

// The single cleanup worker probes at most 128 pass entries per tick, issues
// no SQL for an empty map, and deletes only unchanged idle entries whose
// tenant has no pending Job Runner work.
func TestJobRunnerPassCleanupRemovesOnlyUnchangedAbsentEntries(t *testing.T) {
	tracer := &jobRunnerTrace{}
	store, admin, _ := newJobRunnerTestStore(t, tracer)
	scheduler := store.jobRunner
	statements := tracer.count("")
	if err := scheduler.cleanupOnce(context.Background()); err != nil || tracer.count("") != statements {
		t.Fatalf("empty cleanup = %v with %d statements; want no SQL", err, tracer.count("")-statements)
	}
	insertJobRunnerRows(t, admin, jobRunnerRow{ID: "qjob_cleanup_present", Workspace: "ws_jr_cleanup_present", Kind: KindRuntimeInput, Session: "sesn_cleanup", Thread: "thrd_cleanup", Sequence: 1, AvailableAt: time.Now().UTC().Add(time.Hour)})
	scheduler.mu.Lock()
	for index := 0; index < 130; index++ {
		scheduler.passes[fmt.Sprintf("ws_jr_cleanup_gone_%03d", index)] = &jobRunnerPass{started: true}
	}
	scheduler.passes["ws_jr_cleanup_present"] = &jobRunnerPass{started: true}
	scheduler.passes["ws_jr_cleanup_busy"] = &jobRunnerPass{started: true, busy: true}
	scheduler.passes["ws_jr_cleanup_replaced"] = &jobRunnerPass{started: true}
	scheduler.passes["ws_jr_cleanup_updated"] = &jobRunnerPass{started: true}
	scheduler.mu.Unlock()
	// While each probe runs, a new pass replaces one absent entry and a turn
	// updates another; neither may be deleted on the stale observation.
	var replacement *jobRunnerPass
	tracer.setOnStart(func(_ context.Context, sql string) {
		if !strings.Contains(sql, "LEFT JOIN LATERAL") {
			return
		}
		scheduler.mu.Lock()
		replacement = &jobRunnerPass{started: true}
		scheduler.passes["ws_jr_cleanup_replaced"] = replacement
		scheduler.passes["ws_jr_cleanup_updated"].generation++
		scheduler.mu.Unlock()
	})
	var firstProbe string
	for tick := 0; tick < 2; tick++ {
		before := tracer.count("LEFT JOIN LATERAL")
		if err := scheduler.cleanupOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if probes := tracer.count("LEFT JOIN LATERAL") - before; probes != 1 {
			t.Fatalf("cleanup tick issued %d probes; want one statement", probes)
		}
		if tick == 0 {
			firstProbe = tracer.lastMatching("LEFT JOIN LATERAL")
		}
	}
	tracer.setOnStart(nil)
	if !strings.Contains(firstProbe, "$128::text") || strings.Contains(firstProbe, "$129::text") {
		t.Fatal("first cleanup probe did not carry exactly 128 keys")
	}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	var remaining []string
	for key := range scheduler.passes {
		remaining = append(remaining, key)
	}
	sortStrings(remaining)
	want := []string{"ws_jr_cleanup_busy", "ws_jr_cleanup_present", "ws_jr_cleanup_replaced", "ws_jr_cleanup_updated"}
	if !reflect.DeepEqual(remaining, want) || scheduler.passes["ws_jr_cleanup_replaced"] != replacement {
		t.Fatalf("passes after cleanup = %v; want %v with the replacement pass kept", remaining, want)
	}
}

// Quiesce rejects new direct leases, cancels an admitted call so it returns
// its committed lease, joins the call and the cleanup worker, and clears the
// continuation state.
func TestJobRunnerSchedulerQuiesceJoinsCallsAndWorker(t *testing.T) {
	paused := make(chan struct{})
	ended := make(chan error, 1)
	joining := make(chan struct{})
	quiesced := make(chan struct{})
	var tryLocks, tryLockEnds int
	var order []string
	var mu sync.Mutex
	record := func(step string) {
		mu.Lock()
		order = append(order, step)
		mu.Unlock()
	}
	tracer := &jobRunnerTrace{onStart: func(ctx context.Context, sql string) {
		if !strings.Contains(sql, "pg_try_advisory_xact_lock") {
			return
		}
		mu.Lock()
		tryLocks++
		second := tryLocks == 2
		mu.Unlock()
		if second {
			close(paused)
			<-ctx.Done()
			ended <- ctx.Err()
			// The cancelled call stays in its statement until quiesce begins
			// joining it; a quiesce that returns first never joined it.
			select {
			case <-joining:
			case <-quiesced:
				t.Error("quiesce returned while an admitted call was still running")
			}
		}
	}, onEnd: func(sql string) {
		if !strings.Contains(sql, "pg_try_advisory_xact_lock") {
			return
		}
		mu.Lock()
		tryLockEnds++
		held := tryLockEnds == 2
		mu.Unlock()
		if held {
			record("admitted call finished its last statement")
		}
	}}
	store, admin, _ := newJobRunnerTestStore(t, tracer)
	store.jobRunner.joiningCalls = func() { close(joining) }
	due := time.Now().UTC().Add(-time.Hour)
	insertJobRunnerRows(t, admin,
		jobRunnerRow{ID: "qjob_quiesce_a", Workspace: "ws_jr_quiesce_a", Kind: KindRuntimeInput, Session: "sesn_qa", Thread: "thrd_q", Sequence: 1, AvailableAt: due},
		jobRunnerRow{ID: "qjob_quiesce_b", Workspace: "ws_jr_quiesce_b", Kind: KindRuntimeInput, Session: "sesn_qb", Thread: "thrd_q", Sequence: 1, AvailableAt: due},
	)
	store.StartJobRunnerScheduler()
	admitted := make(chan LeaseJobRunnerJobsResult, 1)
	go func() {
		result, err := store.LeaseJobRunnerJobs(context.Background(), LeaseJobRunnerJobsRequest{LeaseOwner: "quiesce", MaxJobs: 2, LeaseDuration: time.Minute})
		if err != nil {
			t.Errorf("admitted call: %v", err)
		}
		admitted <- result
	}()
	<-paused
	go func() {
		store.QuiesceJobRunnerScheduler()
		record("quiesce returned")
		close(quiesced)
	}()
	select {
	case <-quiesced:
	case <-time.After(5 * time.Second):
		t.Fatal("quiesce did not cancel and join the admitted call")
	}
	mu.Lock()
	steps := append([]string(nil), order...)
	mu.Unlock()
	if want := []string{"admitted call finished its last statement", "quiesce returned"}; !reflect.DeepEqual(steps, want) {
		t.Fatalf("quiesce order = %v; want %v", steps, want)
	}
	// Quiesce itself, not the call's own one-second budget, ended the call.
	if cause := <-ended; !errors.Is(cause, context.Canceled) {
		t.Fatalf("admitted call ended by %v; want cancellation by quiesce", cause)
	}
	select {
	case result := <-admitted:
		assertJobIDs(t, result.Jobs, "qjob_quiesce_a")
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled admitted call did not return its committed lease")
	}
	select {
	case <-store.jobRunner.workerDone:
	default:
		t.Fatal("quiesce returned before joining the cleanup worker")
	}
	store.jobRunner.mu.Lock()
	entries := len(store.jobRunner.passes)
	store.jobRunner.mu.Unlock()
	if entries != 0 {
		t.Fatalf("quiesced scheduler kept %d pass entries", entries)
	}
	if _, err := store.LeaseJobRunnerJobs(context.Background(), LeaseJobRunnerJobsRequest{LeaseOwner: "late", MaxJobs: 1, LeaseDuration: time.Minute}); !errors.Is(err, ErrJobRunnerSchedulerClosed) {
		t.Fatalf("lease after quiesce = %v; want rejection", err)
	}
}

// Release of an undispatched direct lease restores the saved attempt count of
// every Runner kind, including clamped finalization attempts, makes the job
// due at database time, wakes Runners, and keeps every other durable fact.
func TestReleaseUnstartedJobRestoresSavedAttemptForEveryRunnerKind(t *testing.T) {
	store, admin, _ := newJobRunnerTestStore(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	due := time.Now().UTC().Add(-time.Hour)
	seedQueueRuntimeInbox(t, admin, "sesn_release_input", "thrd_release", "rin_release", "messages", `["evt_release"]`, 1, 1)
	seedJobRunnerSession(t, admin, "sesn_release_deleted", "deleted", "idle")
	insertJobRunnerRows(t, admin,
		jobRunnerRow{ID: "qjob_rel_input", Workspace: "default", Kind: KindRuntimeInput, Session: "sesn_release_input", Thread: "thrd_release", Sequence: 1, AvailableAt: due, AttemptCount: 2, MaxAttempts: 5},
		jobRunnerRow{ID: "qjob_rel_delete", Workspace: "default", Kind: KindSessionDeleteCleanup, Session: "sesn_release_deleted", Sequence: 1, AvailableAt: due},
		jobRunnerRow{ID: "qjob_rel_recovery", Workspace: "ws_jr_release_recovery", Kind: KindRuntimeRecovery, Session: "sesn_rel_recovery", Thread: "thrd_release", Sequence: 1, AvailableAt: due},
		jobRunnerRow{ID: "qjob_rel_config", Workspace: "ws_jr_release_config", Kind: KindRuntimeConfigUpdate, Session: "sesn_rel_config", Sequence: 1, AvailableAt: due, AttemptCount: 1},
		jobRunnerRow{ID: "qjob_rel_cleanup", Workspace: "ws_jr_release_cleanup", Kind: KindCleanupSession, Session: "sesn_rel_cleanup", Sequence: 1, AvailableAt: due, AttemptCount: 3, MaxAttempts: 3},
		jobRunnerRow{ID: "qjob_rel_interrupt", Workspace: "ws_jr_release_interrupt", Kind: KindRuntimeInput, Control: ControlClassInterrupt, Session: "sesn_rel_interrupt", Thread: "thrd_release", Sequence: 1, AvailableAt: due, AttemptCount: 3, MaxAttempts: 3},
	)
	if _, err := admin.Exec(`UPDATE queue_jobs SET defer_count = 2, last_error_kind = 'fixture', last_error_message = 'fixture error', dedupe_key = 'dedupe:' || id`); err != nil {
		t.Fatal(err)
	}
	before := queueJobFacts(t, admin)
	inboxBefore := queueInboxFacts(t, admin)

	notifications := make(chan string, 16)
	ready := make(chan struct{})
	listenCtx, stopListen := context.WithCancel(ctx)
	defer stopListen()
	go func() {
		_ = (PostgreSQLNotificationListener{Client: store.client}).Listen(listenCtx, NotificationChannel, func() { close(ready) }, func(payload string) { notifications <- payload })
	}()
	<-ready

	var leased []*Job
	for call := 0; call < 3 && len(leased) < 6; call++ {
		leased = append(leased, leaseJobRunner(t, store, 6).Jobs...)
	}
	if len(leased) != 6 {
		t.Fatalf("direct leases = %d; want all six fixtures", len(leased))
	}
	var dbBefore time.Time
	if err := admin.QueryRow(`SELECT clock_timestamp()`).Scan(&dbBefore); err != nil {
		t.Fatal(err)
	}
	for _, job := range leased {
		released, err := store.ReleaseUnstartedJob(ctx, ReleaseUnstartedJobRequest{WorkspaceID: job.WorkspaceID, JobID: job.ID, LeaseToken: job.LeaseToken})
		if err != nil || !released {
			t.Fatalf("release %s = %v/%v; want released", job.ID, released, err)
		}
		if again, err := store.ReleaseUnstartedJob(ctx, ReleaseUnstartedJobRequest{WorkspaceID: job.WorkspaceID, JobID: job.ID, LeaseToken: job.LeaseToken}); err != nil || again {
			t.Fatalf("duplicate release %s = %v/%v; want not updated", job.ID, again, err)
		}
		select {
		case payload := <-notifications:
			if payload != ConsumerClassJobRunner {
				t.Fatalf("release wake = %q", payload)
			}
		case <-ctx.Done():
			t.Fatalf("release of %s published no Runner wake", job.ID)
		}
	}
	after := queueJobFacts(t, admin)
	for id, fact := range after {
		want := before[id]
		if fact.status != StatusPending || fact.leaseToken.Valid || fact.previous.Valid || fact.availableAt.Before(dbBefore) {
			t.Fatalf("%s after release = %+v; want pending, due at release time, without lease fields", id, fact)
		}
		fact.availableAt, want.availableAt = time.Time{}, time.Time{}
		fact.updatedAt, want.updatedAt = time.Time{}, time.Time{}
		if fact != want {
			t.Fatalf("%s durable facts changed: before %+v after %+v", id, want, fact)
		}
	}
	if inboxAfter := queueInboxFacts(t, admin); inboxAfter != inboxBefore {
		t.Fatalf("Inbox changed: %q -> %q", inboxBefore, inboxAfter)
	}
}

// A lease that expires while release waits for the row lock is not refunded:
// expiry is compared with database time sampled after the wait.
func TestReleaseUnstartedJobComparesExpiryWithTimeAfterRowLock(t *testing.T) {
	store, admin, _ := newJobRunnerTestStore(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	insertJobRunnerRows(t, admin, jobRunnerRow{ID: "qjob_release_wait", Workspace: "ws_jr_release_wait", Kind: KindRuntimeInput, Session: "sesn_release_wait", Thread: "thrd_wait", Sequence: 1, AvailableAt: time.Now().UTC().Add(-time.Hour)})
	job := leaseJobRunner(t, store, 1).Jobs[0]
	blocker, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback() }()
	var blockerPID int
	if err := blocker.QueryRowContext(ctx, `SELECT pg_backend_pid() FROM queue_jobs WHERE id = $1 FOR UPDATE`, job.ID).Scan(&blockerPID); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		released, err := store.ReleaseUnstartedJob(ctx, ReleaseUnstartedJobRequest{WorkspaceID: job.WorkspaceID, JobID: job.ID, LeaseToken: job.LeaseToken})
		if err == nil && released {
			err = errors.New("release refunded a lease that expired during its row-lock wait")
		}
		result <- err
	}()
	waitForQueueSessionLockWaiters(t, admin, blockerPID, 1)
	if _, err := blocker.ExecContext(ctx, `UPDATE queue_jobs SET leased_until = clock_timestamp() WHERE id = $1`, job.ID); err != nil {
		t.Fatal(err)
	}
	if err := blocker.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if attempts, previous := queueJobAttemptState(t, admin, job.ID); attempts != 1 || !previous.Valid || queueJobStatus(t, admin, job.WorkspaceID, job.ID) != StatusLeased {
		t.Fatalf("expired lease after refused release: attempts=%d provenance=%v", attempts, previous)
	}
}

// Defer clears direct-lease provenance and a workspace-scoped Lease never
// records it, so neither the deferred token nor the later Lease token can be
// refunded.
func TestReleaseUnstartedJobRefundsNeitherDeferredNorWorkspaceLeaseTokens(t *testing.T) {
	store, admin, _ := newJobRunnerTestStore(t, nil)
	ctx := context.Background()
	ws := workspace.ID("ws_jr_release_defer")
	payload := `{"workspace_id":"ws_jr_release_defer","session_id":"sesn_release_defer","config_generation":4}`
	mustEnqueue(t, store, EnqueueRequest{ID: "qjob_release_defer", WorkspaceID: ws, Kind: KindRuntimeConfigUpdate, PartitionKey: FormatSessionPartitionKey(ws, "sesn_release_defer"), DedupeKey: FormatRuntimeConfigUpdateDedupeKey(ws, "sesn_release_defer", "4"), PayloadJSON: []byte(payload), Now: time.Now().UTC().Add(-time.Hour)})
	direct := leaseJobRunner(t, store, 1).Jobs[0]
	if deferred, err := store.Defer(ctx, DeferRequest{WorkspaceID: ws, JobID: direct.ID, LeaseToken: direct.LeaseToken}); err != nil || !deferred {
		t.Fatalf("Defer = %v/%v", deferred, err)
	}
	legacy := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindRuntimeConfigUpdate}, LeaseOwner: "sandbox-style", MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC().Add(time.Hour)})
	if released, err := store.ReleaseUnstartedJob(ctx, ReleaseUnstartedJobRequest{WorkspaceID: ws, JobID: direct.ID, LeaseToken: direct.LeaseToken}); err != nil || released {
		t.Fatalf("release of the deferred direct token = %v/%v; want not updated", released, err)
	}
	if _, err := store.ReleaseUnstartedJob(ctx, ReleaseUnstartedJobRequest{WorkspaceID: ws, JobID: legacy.ID, LeaseToken: legacy.LeaseToken}); !IsPreconditionError(err) {
		t.Fatalf("release of a Lease token = %v; want a precondition failure", err)
	}
	if _, err := store.ReleaseUnstartedJob(ctx, ReleaseUnstartedJobRequest{WorkspaceID: ws, JobID: legacy.ID}); !IsValidationError(err) {
		t.Fatalf("release without a token = %v; want invalid input", err)
	}
	attempts, previous := queueJobAttemptState(t, admin, legacy.ID)
	if attempts != 1 || previous.Valid || queueJobStatus(t, admin, ws, legacy.ID) != StatusLeased {
		t.Fatalf("after refused releases attempts=%d provenance=%v; want the Lease custody unchanged", attempts, previous)
	}
}

// Every Queue transition out of a direct lease clears its private provenance;
// the custody CHECK rejects any exit that would leave it behind.
func TestDirectLeaseProvenanceIsClearedByEveryQueueTransitionOutOfLeased(t *testing.T) {
	type transition struct {
		name string
		kind string
		run  func(t *testing.T, store *PostgreSQLQueueStore, admin *sql.DB, job *Job) bool
	}
	ctx := context.Background()
	transitions := []transition{
		{"ack", KindRuntimeInput, func(t *testing.T, store *PostgreSQLQueueStore, _ *sql.DB, job *Job) bool {
			ok, err := store.Ack(ctx, AckRequest{WorkspaceID: job.WorkspaceID, JobID: job.ID, LeaseToken: job.LeaseToken})
			return mustTransition(t, ok, err)
		}},
		{"retry", KindRuntimeInput, func(t *testing.T, store *PostgreSQLQueueStore, _ *sql.DB, job *Job) bool {
			ok, err := store.Retry(ctx, RetryRequest{WorkspaceID: job.WorkspaceID, JobID: job.ID, LeaseToken: job.LeaseToken, ErrorKind: "fixture"})
			return mustTransition(t, ok, err)
		}},
		{"retry exhaustion", KindRuntimeRecovery, func(t *testing.T, store *PostgreSQLQueueStore, admin *sql.DB, job *Job) bool {
			if _, err := admin.Exec(`UPDATE queue_jobs SET max_attempts = 1 WHERE id = $1`, job.ID); err != nil {
				t.Fatal(err)
			}
			ok, err := store.Retry(ctx, RetryRequest{WorkspaceID: job.WorkspaceID, JobID: job.ID, LeaseToken: job.LeaseToken, ErrorKind: "fixture"})
			return mustTransition(t, ok, err)
		}},
		{"retry finalization", KindCleanupSession, func(t *testing.T, store *PostgreSQLQueueStore, admin *sql.DB, job *Job) bool {
			if _, err := admin.Exec(`UPDATE queue_jobs SET max_attempts = 1 WHERE id = $1`, job.ID); err != nil {
				t.Fatal(err)
			}
			ok, err := store.Retry(ctx, RetryRequest{WorkspaceID: job.WorkspaceID, JobID: job.ID, LeaseToken: job.LeaseToken, ErrorKind: "fixture"})
			return mustTransition(t, ok, err)
		}},
		{"defer", KindRuntimeConfigUpdate, func(t *testing.T, store *PostgreSQLQueueStore, _ *sql.DB, job *Job) bool {
			ok, err := store.Defer(ctx, DeferRequest{WorkspaceID: job.WorkspaceID, JobID: job.ID, LeaseToken: job.LeaseToken})
			return mustTransition(t, ok, err)
		}},
		{"dead letter", KindCleanupSession, func(t *testing.T, store *PostgreSQLQueueStore, _ *sql.DB, job *Job) bool {
			ok, err := store.DeadLetter(ctx, DeadLetterRequest{WorkspaceID: job.WorkspaceID, JobID: job.ID, LeaseToken: job.LeaseToken, ErrorKind: "fixture"})
			return mustTransition(t, ok, err)
		}},
		{"reclaim", KindRuntimeInput, func(t *testing.T, store *PostgreSQLQueueStore, admin *sql.DB, job *Job) bool {
			expireQueueJobLease(t, admin, job.WorkspaceID, job.ID)
			count, err := store.ReclaimExpiredLeases(ctx, ReclaimExpiredLeasesRequest{})
			return mustTransition(t, count == 1, err)
		}},
		{"barrier-stale cancellation", KindRuntimeInput, func(t *testing.T, store *PostgreSQLQueueStore, _ *sql.DB, job *Job) bool {
			var ok bool
			err := store.client.WithWorkspaceTx(ctx, job.WorkspaceID.String(), "queue.test_cancel_custody", func(tx *dbconnect.Tx) error {
				var err error
				ok, err = CancelLeasedRuntimeInputCustodyTx(ctx, tx, CancelLeasedRuntimeInputRequest{Lease: exactLeaseOf(job), SessionID: "sesn_transition", RuntimeInputID: "rin_transition", InputKind: "messages"})
				return err
			})
			return mustTransition(t, ok, err)
		}},
		{"barrier deferral", KindRuntimeInput, func(t *testing.T, store *PostgreSQLQueueStore, _ *sql.DB, job *Job) bool {
			var ok bool
			err := store.client.WithWorkspaceTx(ctx, job.WorkspaceID.String(), "queue.test_defer_custody", func(tx *dbconnect.Tx) error {
				var err error
				ok, err = DeferLeasedRuntimeInputCustodyTx(ctx, tx, DeferLeasedRuntimeInputRequest{Lease: exactLeaseOf(job), SessionID: "sesn_transition", RuntimeInputID: "rin_transition", InputKind: "messages"})
				return err
			})
			return mustTransition(t, ok, err)
		}},
	}
	for _, tc := range transitions {
		t.Run(tc.name, func(t *testing.T) {
			store, admin, _ := newJobRunnerTestStore(t, nil)
			ws := workspace.DefaultID
			seedQueueRuntimeInbox(t, admin, "sesn_transition", "thrd_transition", "rin_transition", "messages", `["evt_transition"]`, 1, 1)
			request := EnqueueRequest{ID: "qjob_transition", WorkspaceID: ws, Kind: tc.kind, PartitionKey: FormatSessionPartitionKey(ws, "sesn_transition"), Now: time.Now().UTC().Add(-time.Hour)}
			switch tc.kind {
			case KindRuntimeInput:
				request.DedupeKey = FormatRuntimeInputDedupeKey(ws, "sesn_transition", "rin_transition")
				request.PayloadJSON = runtimeInputPayload(t, ws, "sesn_transition", "thrd_transition", "rin_transition", "messages", 1, 1)
			case KindRuntimeRecovery:
				recovery, err := NewRuntimeRecoveryEnqueueRequest(ws, "sesn_transition", "thrd_transition", "evt_transition", request.Now)
				if err != nil {
					t.Fatal(err)
				}
				request = recovery
			case KindRuntimeConfigUpdate:
				request.DedupeKey = FormatRuntimeConfigUpdateDedupeKey(ws, "sesn_transition", "1")
				request.PayloadJSON = []byte(`{"workspace_id":"default","session_id":"sesn_transition","config_generation":1}`)
			case KindCleanupSession:
				request.DedupeKey = FormatCleanupSessionDedupeKey(ws, "sesn_transition", "cln_transition")
				request.PayloadJSON = []byte(`{"workspace_id":"default","session_id":"sesn_transition","cleanup_job_id":"cln_transition"}`)
			}
			mustEnqueue(t, store, request)
			jobs := leaseJobRunner(t, store, 1).Jobs
			if len(jobs) != 1 {
				t.Fatalf("direct lease = %v", jobs)
			}
			if _, previous := queueJobAttemptState(t, admin, jobs[0].ID); !previous.Valid {
				t.Fatal("direct lease recorded no provenance")
			}
			if !tc.run(t, store, admin, jobs[0]) {
				t.Fatalf("%s did not leave leased custody", tc.name)
			}
			if status, previous := queueJobStatus(t, admin, ws, jobs[0].ID), queueJobProvenance(t, admin, jobs[0].ID); status == StatusLeased || previous.Valid {
				t.Fatalf("%s left status=%s provenance=%v", tc.name, status, previous)
			}
		})
	}
}

func newJobRunnerTestStore(t *testing.T, tracer pgx.QueryTracer) (*PostgreSQLQueueStore, *sql.DB, *storagetest.WorkloadDB) {
	t.Helper()
	admin := storagetest.NewPostgreSQLAdminDB(t)
	workload := storagetest.OpenWorkloadDB(t, admin, "queue")
	db := workload.DB
	if tracer != nil {
		db = workload.OpenWorkload(t, "queue", tracer)
	}
	return NewPostgreSQLStore(dbconnect.NewClientForTesting(db)), admin, workload
}

func leaseJobRunner(t *testing.T, store *PostgreSQLQueueStore, maxJobs int) LeaseJobRunnerJobsResult {
	t.Helper()
	result, err := store.LeaseJobRunnerJobs(context.Background(), LeaseJobRunnerJobsRequest{LeaseOwner: "job-runner-test", MaxJobs: maxJobs, LeaseDuration: time.Minute})
	if err != nil {
		t.Fatalf("LeaseJobRunnerJobs: %v", err)
	}
	return result
}

func assertJobIDs(t *testing.T, jobs []*Job, want ...string) {
	t.Helper()
	got := make([]string, 0, len(jobs))
	for _, job := range jobs {
		got = append(got, job.ID)
		if job.Status != StatusLeased || job.LeaseToken == "" {
			t.Fatalf("job %s returned without live custody", job.ID)
		}
	}
	if len(got) != len(want) || (len(got) > 0 && !reflect.DeepEqual(got, want)) {
		t.Fatalf("leased jobs = %v; want %v", got, want)
	}
}

func jobRunnerPassSnapshot(store *PostgreSQLQueueStore, tenant string) *jobRunnerPass {
	store.jobRunner.mu.Lock()
	defer store.jobRunner.mu.Unlock()
	pass := store.jobRunner.passes[tenant]
	if pass == nil {
		return nil
	}
	snapshot := *pass
	return &snapshot
}

type jobRunnerRow struct {
	ID, Workspace, Kind, Session, Thread, Control, Status string
	Sequence                                              int64
	Priority                                              int
	AvailableAt                                           time.Time
	AttemptCount, MaxAttempts                             int
}

func insertJobRunnerRows(t *testing.T, admin *sql.DB, rows ...jobRunnerRow) {
	t.Helper()
	created := time.Now().UTC().Add(-2 * time.Hour)
	for _, row := range rows {
		scope, thread := DeliveryScopeSession, any(nil)
		if row.Kind == KindRuntimeInput || row.Kind == KindRuntimeRecovery {
			scope, thread = DeliveryScopeThread, row.Thread
		}
		var token, owner, leasedAt, leasedUntil any
		status := defaultString(row.Status, StatusPending)
		attempts := row.AttemptCount
		if status == StatusLeased {
			token, owner, leasedAt, leasedUntil = "qlt_fixture_"+row.ID, "fixture", created, time.Now().UTC().Add(time.Hour)
			attempts = max(attempts, 1)
		}
		maxAttempts := row.MaxAttempts
		if maxAttempts == 0 {
			maxAttempts = 10
		}
		if _, err := admin.Exec(`INSERT INTO queue_jobs (
			id, workspace_id, kind, partition_key, queue_partition_sequence, causal_session_id,
			delivery_scope, delivery_thread_id, control_class, payload_version, status, payload_json,
			priority, lease_token, leased_by, leased_at, leased_until, attempt_count, max_attempts,
			available_at, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 1, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $20)`,
			row.ID, row.Workspace, row.Kind, FormatSessionPartitionKey(workspace.ID(row.Workspace), row.Session), row.Sequence, row.Session,
			scope, thread, defaultString(row.Control, ControlClassOrdinary), status, `{"fixture":"`+row.ID+`"}`,
			row.Priority, token, owner, leasedAt, leasedUntil, attempts, maxAttempts, row.AvailableAt, created,
		); err != nil {
			t.Fatalf("insert Queue fixture %s: %v", row.ID, err)
		}
	}
}

// insertJobRunnerSeries inserts count pending runtime_input rows of one tenant,
// each in its own Session, in ascending scan-key order.
func insertJobRunnerSeries(t *testing.T, admin *sql.DB, tenant string, count int, priority int, availableAt time.Time) {
	t.Helper()
	if _, err := admin.Exec(`INSERT INTO queue_jobs (
		id, workspace_id, kind, partition_key, queue_partition_sequence, causal_session_id,
		delivery_scope, delivery_thread_id, control_class, payload_version, status, payload_json,
		priority, attempt_count, max_attempts, available_at, created_at, updated_at
	) SELECT 'qjob_' || $1 || '_' || lpad(value::text, 3, '0'), $1, 'runtime_input',
	         'session:' || $1 || ':sesn_' || lpad(value::text, 3, '0'), 1, 'sesn_' || lpad(value::text, 3, '0'),
	         'thread', 'thrd_series', 'ordinary', 1, 'pending', '{}', $3, 0, 10, $4, $5, $5
	    FROM generate_series(1, $2::integer) AS value`,
		tenant, count, priority, availableAt, time.Now().UTC().Add(-2*time.Hour),
	); err != nil {
		t.Fatalf("insert Queue series for %s: %v", tenant, err)
	}
}

func seedJobRunnerSession(t *testing.T, admin *sql.DB, sessionID string, lifecycle string, status string) {
	t.Helper()
	const created = "2026-01-01T00:00:00Z"
	for _, statement := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO workspaces (id, type, name, created_at) VALUES ('default', 'workspace', 'default', $1) ON CONFLICT (id) DO NOTHING`, []any{created}},
		{`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at) VALUES ('default', $1, $1, 1, $2, $2)`, []any{"agent_" + sessionID, created}},
		{`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at) VALUES ('default', $1, $2, 1, '{}', $3, $4)`, []any{"agv_" + sessionID, "agent_" + sessionID, "hash_" + sessionID, created}},
		{`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at) VALUES ('default', $1, $1, '{}', $2, $2)`, []any{"env_" + sessionID, created}},
		{`INSERT INTO sessions (workspace_id, id, main_thread_id, type, status, lifecycle_state, agent_id, agent_version, environment_id, installed_tools_json, created_at, updated_at) VALUES ('default', $1, $2, 'session', $3, $4, $5, 1, $6, '{}', $7, $7)`, []any{sessionID, "thrd_" + sessionID, status, lifecycle, "agent_" + sessionID, "env_" + sessionID, created}},
	} {
		if _, err := admin.Exec(statement.query, statement.args...); err != nil {
			t.Fatalf("seed Session %s: %v", sessionID, err)
		}
	}
}

func queueJobAttemptState(t *testing.T, admin *sql.DB, jobID string) (int, sql.NullInt64) {
	t.Helper()
	var attempts int
	var previous sql.NullInt64
	if err := admin.QueryRow(`SELECT attempt_count, lease_previous_attempt_count FROM queue_jobs WHERE id = $1`, jobID).Scan(&attempts, &previous); err != nil {
		t.Fatalf("read attempt state of %s: %v", jobID, err)
	}
	return attempts, previous
}

func queueJobProvenance(t *testing.T, admin *sql.DB, jobID string) sql.NullInt64 {
	t.Helper()
	_, previous := queueJobAttemptState(t, admin, jobID)
	return previous
}

type queueJobFact struct {
	status, kind, partition, payload, dedupe, lastErrorKind, lastErrorMessage string
	sequence                                                                  int64
	attempts, deferCount, maxAttempts, priority                               int
	leaseToken                                                                sql.NullString
	previous                                                                  sql.NullInt64
	availableAt, createdAt, updatedAt                                         time.Time
}

func queueJobFacts(t *testing.T, admin *sql.DB) map[string]queueJobFact {
	t.Helper()
	rows, err := admin.Query(`SELECT id, status, kind, partition_key, payload_json, COALESCE(dedupe_key, ''),
	        COALESCE(last_error_kind, ''), COALESCE(last_error_message, ''), queue_partition_sequence,
	        attempt_count, defer_count, max_attempts, priority, lease_token, lease_previous_attempt_count,
	        available_at, created_at, updated_at
	   FROM queue_jobs`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	facts := map[string]queueJobFact{}
	for rows.Next() {
		var id string
		var fact queueJobFact
		if err := rows.Scan(&id, &fact.status, &fact.kind, &fact.partition, &fact.payload, &fact.dedupe,
			&fact.lastErrorKind, &fact.lastErrorMessage, &fact.sequence, &fact.attempts, &fact.deferCount,
			&fact.maxAttempts, &fact.priority, &fact.leaseToken, &fact.previous, &fact.availableAt, &fact.createdAt, &fact.updatedAt); err != nil {
			t.Fatal(err)
		}
		fact.availableAt, fact.createdAt, fact.updatedAt = fact.availableAt.UTC(), fact.createdAt.UTC(), fact.updatedAt.UTC()
		facts[id] = fact
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return facts
}

func queueInboxFacts(t *testing.T, admin *sql.DB) string {
	t.Helper()
	var facts string
	if err := admin.QueryRow(`SELECT COALESCE(string_agg(runtime_input_id || ':' || status || ':' || updated_at::text, ',' ORDER BY runtime_input_id), '') FROM session_runtime_inbox`).Scan(&facts); err != nil {
		t.Fatal(err)
	}
	return facts
}

func exactLeaseOf(job *Job) ExactLeaseRequest {
	return ExactLeaseRequest{WorkspaceID: job.WorkspaceID, JobID: job.ID, LeaseToken: job.LeaseToken, Kind: job.Kind, PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey}
}

func mustTransition(t *testing.T, ok bool, err error) bool {
	t.Helper()
	if err != nil {
		t.Fatalf("transition: %v", err)
	}
	return ok
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}

// jobRunnerTrace records statements issued through a traced Queue-role pool
// and optionally runs a barrier when one starts or an observer when it ends.
type jobRunnerTrace struct {
	mu         sync.Mutex
	statements []string
	onStart    func(ctx context.Context, sql string)
	onEnd      func(sql string)
}

type jobRunnerTraceSQLKey struct{}

func (tr *jobRunnerTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	tr.mu.Lock()
	tr.statements = append(tr.statements, data.SQL)
	onStart := tr.onStart
	tr.mu.Unlock()
	if onStart != nil {
		onStart(ctx, data.SQL)
	}
	return context.WithValue(ctx, jobRunnerTraceSQLKey{}, data.SQL)
}

func (tr *jobRunnerTrace) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if tr.onEnd == nil {
		return
	}
	query, _ := ctx.Value(jobRunnerTraceSQLKey{}).(string)
	tr.onEnd(query)
}

func (tr *jobRunnerTrace) setOnStart(onStart func(ctx context.Context, sql string)) {
	tr.mu.Lock()
	tr.onStart = onStart
	tr.mu.Unlock()
}

func (tr *jobRunnerTrace) count(fragment string) int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	count := 0
	for _, statement := range tr.statements {
		if strings.Contains(statement, fragment) {
			count++
		}
	}
	return count
}

func (tr *jobRunnerTrace) lastMatching(fragment string) string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	for index := len(tr.statements) - 1; index >= 0; index-- {
		if strings.Contains(tr.statements[index], fragment) {
			return tr.statements[index]
		}
	}
	return ""
}

type jobRunnerPlanEvidence struct {
	indexes             map[string]bool
	indexConds          []string
	queueSeqScans       int
	sorts               int
	maxQueueRows        float64
	maxQueueRowsRemoved float64
	raw                 string
}

// explainJobRunnerStatement runs one discovery statement under the store's
// Queue-role pool inside the same read-only queue_maintenance transaction the
// scheduler uses, with sequential scans and sorts disabled.
func explainJobRunnerStatement(t *testing.T, store *PostgreSQLQueueStore, query string, args ...any) jobRunnerPlanEvidence {
	t.Helper()
	evidence := jobRunnerPlanEvidence{indexes: map[string]bool{}}
	err := store.withJobRunnerDiscoveryTx(context.Background(), "queue.test_explain", time.Second, func(tx *dbconnect.Tx) error {
		if _, err := tx.Exec(context.Background(), `SET LOCAL enable_seqscan = off; SET LOCAL enable_sort = off`); err != nil {
			return err
		}
		return tx.QueryRow(context.Background(), "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, FORMAT JSON) "+query, args...).Scan(&evidence.raw)
	})
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	var documents []struct {
		Plan map[string]any `json:"Plan"`
	}
	if err := json.Unmarshal([]byte(evidence.raw), &documents); err != nil || len(documents) != 1 {
		t.Fatalf("decode plan: %v", err)
	}
	walkQueueLeasePlan(documents[0].Plan, func(node map[string]any) {
		nodeType, _ := node["Node Type"].(string)
		if nodeType == "Sort" || nodeType == "Incremental Sort" {
			evidence.sorts++
		}
		if index, _ := node["Index Name"].(string); index != "" {
			evidence.indexes[index] = true
		}
		if cond, _ := node["Index Cond"].(string); cond != "" {
			evidence.indexConds = append(evidence.indexConds, cond)
		}
		if relation, _ := node["Relation Name"].(string); relation != "queue_jobs" {
			return
		}
		if nodeType == "Seq Scan" {
			evidence.queueSeqScans++
		}
		rows, _ := node["Actual Rows"].(float64)
		removed, _ := node["Rows Removed by Filter"].(float64)
		recheck, _ := node["Rows Removed by Index Recheck"].(float64)
		evidence.maxQueueRows = max(evidence.maxQueueRows, rows)
		evidence.maxQueueRowsRemoved = max(evidence.maxQueueRowsRemoved, removed+recheck)
	})
	return evidence
}
