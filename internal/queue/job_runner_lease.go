package queue

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage"
)

// Direct Job Runner leasing: Queue discovers, rotates and leases the five Job
// Runner kinds across workspaces; the Runner supplies only free capacity.
//
//	call -> turn: snapshot (last_workspace, generation) -> tenant index seek
//	           -> CAS commit of the cursor and busy reservation
//	           -> bounded raw window of that tenant's pass
//	           -> one exact scoped lease transaction per examined candidate
//	           -> advance only the examined prefix; release the reservation
//
// Discovery reads only queue_jobs through the partial indexes
// idx_queue_job_runner_scan and idx_queue_job_runner_due, in read-only
// transactions with the queue_maintenance setting. Each exact lease runs in
// its own workspace transaction and rechecks the shared
// leaseEligibilityPredicate against fresh database time, so scan order never
// overrides causal or interrupt barriers.

const jobRunnerKindSQLList = "'runtime_input', 'runtime_recovery', 'runtime_config_update', 'cleanup_session', 'session_delete_cleanup'"

// jobRunnerPendingPredicate matches the predicate of both Job Runner partial
// indexes; discovery statements must keep it verbatim to use them.
const jobRunnerPendingPredicate = "status = 'pending' AND kind IN (" + jobRunnerKindSQLList + ")"

// The scan key K orders one tenant's pending Job Runner rows. negative_priority
// is the stored -(priority::bigint); comparing the plain column keeps every
// window bound a leakproof index condition under row-level security.
const jobRunnerScanKey = "negative_priority, available_at, partition_key, queue_partition_sequence, id"

const (
	jobRunnerNextTenantSQL = `SELECT workspace_id FROM queue_jobs
	  WHERE ` + jobRunnerPendingPredicate + ` AND workspace_id > $1
	  ORDER BY workspace_id LIMIT 1`
	jobRunnerFirstTenantSQL = `SELECT workspace_id FROM queue_jobs
	  WHERE ` + jobRunnerPendingPredicate + `
	  ORDER BY workspace_id LIMIT 1`
	jobRunnerPassUpperSQL = `SELECT ` + jobRunnerScanKey + `, clock_timestamp()
	   FROM queue_jobs
	  WHERE ` + jobRunnerPendingPredicate + ` AND workspace_id = $1
	  ORDER BY negative_priority DESC, available_at DESC, partition_key DESC,
	           queue_partition_sequence DESC, id DESC
	  LIMIT 1`
	jobRunnerNextFutureSQL = `SELECT available_at, statement_timestamp()
	   FROM queue_jobs
	  WHERE ` + jobRunnerPendingPredicate + ` AND available_at > statement_timestamp()
	  ORDER BY available_at, id LIMIT 1`
)

// jobRunnerWindowSQL fetches raw candidates before any time, lifecycle or
// barrier evaluation: the LIMIT applies directly to the index range
// (after, upper] of one tenant.
func jobRunnerWindowSQL(afterBound bool) string {
	lower := ""
	if afterBound {
		lower = `
	    AND (` + jobRunnerScanKey + `) > ($8::bigint, $9::timestamptz, $10::text, $11::bigint, $12::text)`
	}
	return `SELECT kind, COALESCE(causal_session_id, ''), ` + jobRunnerScanKey + `, created_at
	   FROM queue_jobs
	  WHERE ` + jobRunnerPendingPredicate + ` AND workspace_id = $1
	    AND (` + jobRunnerScanKey + `) <= ($2::bigint, $3::timestamptz, $4::text, $5::bigint, $6::text)` + lower + `
	  ORDER BY ` + jobRunnerScanKey + `
	  LIMIT $7`
}

// jobRunnerAbsentTenantsSQL returns which of count tenant keys no longer hold
// any pending Job Runner row: one lateral probe per key for the tenant's first
// raw candidate in scan order. The ordered LIMIT keeps each probe a seek on
// idx_queue_job_runner_scan; an EXISTS form lets the planner hash every
// pending Job Runner row instead.
func jobRunnerAbsentTenantsSQL(count int) string {
	values := make([]string, count)
	for index := range values {
		values[index] = "($" + strconv.Itoa(index+1) + "::text)"
	}
	return `SELECT tenant.workspace_id
	   FROM (VALUES ` + strings.Join(values, ", ") + `) AS tenant(workspace_id)
	   LEFT JOIN LATERAL (
	     SELECT true AS present FROM queue_jobs
	      WHERE ` + jobRunnerPendingPredicate + ` AND workspace_id = tenant.workspace_id
	      ORDER BY ` + jobRunnerScanKey + `
	      LIMIT 1
	   ) probe ON true
	  WHERE probe.present IS NULL`
}

type jobRunnerKey struct {
	negativePriority  int64
	availableAt       time.Time
	partitionKey      string
	partitionSequence int64
	id                string
}

func (k jobRunnerKey) equal(other jobRunnerKey) bool {
	return k.negativePriority == other.negativePriority && k.availableAt.Equal(other.availableAt) &&
		k.partitionKey == other.partitionKey && k.partitionSequence == other.partitionSequence && k.id == other.id
}

type jobRunnerCandidate struct {
	key             jobRunnerKey
	kind            string
	causalSessionID string
	createdAt       time.Time
}

// jobRunnerPass is one tenant's continuation. It holds bounded key strings
// only, never payload, and is created or updated only by the call that holds
// its busy reservation.
type jobRunnerPass struct {
	started    bool
	upper      jobRunnerKey
	hasAfter   bool
	after      jobRunnerKey
	passNow    time.Time
	examined   int
	busy       bool
	generation uint64
}

// jobRunnerScheduler is the single per-process owner of Job Runner tenant
// rotation. Its mutex protects cursor, reservations and pass state only; it
// is never held across database I/O or Session locks. Process restart resets
// preference and progress, never Queue jobs or leases.
type jobRunnerScheduler struct {
	store *PostgreSQLQueueStore

	mu            sync.Mutex
	lastWorkspace string
	generation    uint64
	passes        map[string]*jobRunnerPass
	cleanupAfter  string
	// The process is idle after a cursor wrap during which no lease committed;
	// any committed lease clears it.
	leasesSinceWrap int
	idle            bool
	closed          bool

	calls       sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	startOnce   sync.Once
	quiesceOnce sync.Once
	workerDone  chan struct{}

	// afterTenantSeek runs between a turn's tenant seek and its cursor commit,
	// outside the mutex. In-package tests use it to interleave another call in
	// that window; production leaves it nil.
	afterTenantSeek func(proposed string)
	// joiningCalls runs as quiesce begins joining admitted calls. In-package
	// tests use it to release a held call only once quiesce waits for it;
	// production leaves it nil.
	joiningCalls func()
}

func newJobRunnerScheduler(store *PostgreSQLQueueStore) *jobRunnerScheduler {
	// #nosec G118 -- the scheduler retains cancel; quiesce cancels admitted calls and the worker, then joins them.
	ctx, cancel := context.WithCancel(context.Background())
	return &jobRunnerScheduler{store: store, passes: map[string]*jobRunnerPass{}, ctx: ctx, cancel: cancel}
}

// LeaseJobRunnerJobs leases up to MaxJobs Runner jobs across workspaces. A
// valid request may receive fewer jobs, including none; that is success.
func (s *PostgreSQLQueueStore) LeaseJobRunnerJobs(ctx context.Context, request LeaseJobRunnerJobsRequest) (LeaseJobRunnerJobsResult, error) {
	if err := ValidateLeaseJobRunnerJobsRequest(request); err != nil {
		return LeaseJobRunnerJobsResult{}, err
	}
	if s == nil || s.client == nil || s.jobRunner == nil {
		return LeaseJobRunnerJobsResult{}, &ValidationError{Message: "queue store is required"}
	}
	return s.jobRunner.lease(ctx, request)
}

// StartJobRunnerScheduler starts the scheduler's one cleanup worker. Queue
// bootstrap calls it once after constructing the store; repeated calls are
// no-ops.
func (s *PostgreSQLQueueStore) StartJobRunnerScheduler() {
	if s == nil || s.jobRunner == nil {
		return
	}
	s.jobRunner.start()
}

// QuiesceJobRunnerScheduler rejects new direct leases, cancels admitted calls
// and the cleanup worker, joins them, and clears continuation state. Queue
// shutdown calls it before the database pool closes.
func (s *PostgreSQLQueueStore) QuiesceJobRunnerScheduler() {
	if s == nil || s.jobRunner == nil {
		return
	}
	s.jobRunner.quiesce()
}

func (s *jobRunnerScheduler) start() {
	s.startOnce.Do(func() {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		s.workerDone = make(chan struct{})
		s.mu.Unlock()
		go s.runCleanup()
	})
}

func (s *jobRunnerScheduler) quiesce() {
	s.quiesceOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		workerDone := s.workerDone
		s.mu.Unlock()
		s.cancel()
		s.joinCalls()
		if workerDone != nil {
			<-workerDone
		}
		s.mu.Lock()
		s.passes = map[string]*jobRunnerPass{}
		s.mu.Unlock()
	})
}

// joinCalls waits until every admitted call has returned.
func (s *jobRunnerScheduler) joinCalls() {
	if s.joiningCalls != nil {
		s.joiningCalls()
	}
	s.calls.Wait()
}

func (s *jobRunnerScheduler) admit() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.calls.Add(1)
	return true
}

type jobRunnerCall struct {
	request           LeaseJobRunnerJobsRequest
	visited           map[string]struct{}
	turns             int
	rawCandidates     int
	leaseTransactions int
	jobs              []*Job
	budgetStopped     bool
	contended         bool
	lockTimeouts      int
	statementTimeouts int
}

func (s *jobRunnerScheduler) lease(ctx context.Context, request LeaseJobRunnerJobsRequest) (LeaseJobRunnerJobsResult, error) {
	if !s.admit() {
		return LeaseJobRunnerJobsResult{}, ErrJobRunnerSchedulerClosed
	}
	defer s.calls.Done()
	callCtx, cancel := context.WithTimeout(ctx, jobRunnerCallBudget)
	defer cancel()
	stopOnQuiesce := context.AfterFunc(s.ctx, cancel)
	defer stopOnQuiesce()

	call := &jobRunnerCall{
		request:           request,
		visited:           make(map[string]struct{}, jobRunnerCallTurns),
		turns:             jobRunnerCallTurns,
		rawCandidates:     jobRunnerCallRawCandidates,
		leaseTransactions: jobRunnerCallLeaseTransactions,
	}
	var failure error
	for len(call.jobs) < request.MaxJobs {
		if call.turns == 0 || call.rawCandidates == 0 || call.leaseTransactions == 0 || callCtx.Err() != nil {
			call.budgetStopped = true
			break
		}
		call.turns--
		completed, err := s.turn(callCtx, call)
		if err != nil {
			if callCtx.Err() != nil || errors.Is(err, errJobRunnerBudgetSpent) {
				call.budgetStopped = true
				break
			}
			failure = err
			break
		}
		if completed {
			break
		}
	}
	result := LeaseJobRunnerJobsResult{
		Jobs:              call.jobs,
		LockTimeouts:      call.lockTimeouts,
		StatementTimeouts: call.statementTimeouts,
	}
	if failure != nil {
		// A later failure never hides earlier commits: the caller receives
		// every committed token and retries soon.
		if len(call.jobs) == 0 {
			return LeaseJobRunnerJobsResult{}, failure
		}
		result.RetryAfter = jobRunnerActiveRetryAfter
		result.StopFailure = failure
		return result, nil
	}
	result.RetryAfter = s.retryAfter(callCtx, call)
	return result, nil
}

// retryAfter is the active hint after a budget or contention stop while the
// process is not idle. Otherwise the caller waits until the next future Job
// Runner job, clamped to 100..1000 ms, or 1000 ms when none exists or the call
// budget is already spent. A call that ends a retained pass at its upper key
// has completed discovery, so a job admitted after that pass began waits up to
// this hint for the next call's fresh pass.
func (s *jobRunnerScheduler) retryAfter(ctx context.Context, call *jobRunnerCall) time.Duration {
	s.mu.Lock()
	idle := s.idle
	s.mu.Unlock()
	if (call.budgetStopped || call.contended) && !idle {
		return jobRunnerActiveRetryAfter
	}
	if ctx.Err() != nil {
		return jobRunnerIdleRetryAfter
	}
	availableAt, now, found, err := s.store.jobRunnerNextFutureJob(ctx)
	if err != nil || !found {
		return jobRunnerIdleRetryAfter
	}
	return min(max(availableAt.Sub(now), jobRunnerActiveRetryAfter), jobRunnerIdleRetryAfter)
}

// turn performs one tenant turn attempt and reports completed when discovery
// has no tenant left for this call: no pending Job Runner work exists, or the
// cursor reached a tenant the call already visited.
func (s *jobRunnerScheduler) turn(ctx context.Context, call *jobRunnerCall) (bool, error) {
	s.mu.Lock()
	last, generation := s.lastWorkspace, s.generation
	s.mu.Unlock()
	tenant, wrapped, err := s.store.jobRunnerSeekTenant(ctx, last)
	if err != nil {
		return false, err
	}
	if s.afterTenantSeek != nil {
		s.afterTenantSeek(tenant)
	}

	s.mu.Lock()
	if s.generation != generation {
		// Another call moved the shared cursor after this snapshot. The stale
		// proposal is discarded and has consumed this turn attempt.
		s.mu.Unlock()
		return false, nil
	}
	if tenant == "" {
		s.mu.Unlock()
		return true, nil
	}
	if _, visited := call.visited[tenant]; visited {
		s.mu.Unlock()
		return true, nil
	}
	call.visited[tenant] = struct{}{}
	s.lastWorkspace = tenant
	s.generation++
	if wrapped {
		s.idle = s.leasesSinceWrap == 0
		s.leasesSinceWrap = 0
	}
	pass := s.passes[tenant]
	if pass != nil && pass.busy {
		// A concurrent call owns this tenant's turn; it is not waited for.
		s.mu.Unlock()
		return false, nil
	}
	if pass == nil {
		pass = &jobRunnerPass{}
		s.passes[tenant] = pass
	}
	pass.busy = true
	state := *pass
	s.mu.Unlock()

	complete := false
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		pass.busy = false
		if s.passes[tenant] != pass {
			return
		}
		if complete {
			delete(s.passes, tenant)
			return
		}
		state.busy = false
		state.generation = pass.generation + 1
		*pass = state
	}()
	state, complete, err = s.tenantTurn(ctx, call, tenant, state)
	return false, err
}

// tenantTurn examines at most one raw window of the tenant's current pass and
// commits at most one job. The returned state advances only through the
// candidates actually examined; complete reports that the pass reached its
// end and the next visit starts a new pass.
func (s *jobRunnerScheduler) tenantTurn(ctx context.Context, call *jobRunnerCall, tenant string, pass jobRunnerPass) (jobRunnerPass, bool, error) {
	if !pass.started {
		upper, passNow, found, err := s.store.jobRunnerStartPass(ctx, tenant)
		if err != nil {
			return pass, false, err
		}
		if !found {
			return pass, true, nil
		}
		pass = jobRunnerPass{started: true, upper: upper, passNow: passNow}
	}
	limit := min(jobRunnerTenantWindow, call.rawCandidates)
	window, err := s.store.jobRunnerWindow(ctx, tenant, pass, limit)
	if err != nil {
		return pass, false, err
	}
	// Every fetched row is charged, even when examination stops early.
	call.rawCandidates -= len(window)
	examined := 0
	for _, candidate := range window {
		if len(call.jobs) >= call.request.MaxJobs || call.leaseTransactions == 0 || ctx.Err() != nil {
			break
		}
		committed := false
		// Work that became due or was admitted after the pass began waits for
		// the next pass; barriers are always rechecked against current state.
		if !candidate.key.availableAt.After(pass.passNow) && !candidate.createdAt.After(pass.passNow) {
			call.leaseTransactions--
			job, outcome, err := s.store.leaseJobRunnerCandidate(ctx, tenant, candidate, call.request)
			if err != nil {
				pass.examined += examined
				return pass, false, err
			}
			switch outcome {
			case jobRunnerLeaseCommitted:
				call.jobs = append(call.jobs, job)
				s.recordLease()
				committed = true
			case jobRunnerLeaseContended:
				call.contended = true
			case jobRunnerLeaseLockTimeout:
				call.contended = true
				call.lockTimeouts++
			case jobRunnerLeaseStatementTimeout:
				call.contended = true
				call.statementTimeouts++
			}
		}
		// An unsuccessful lease, a lock miss and a skipped later-pass row are
		// all examined: the cursor moves past them.
		examined++
		pass.hasAfter = true
		pass.after = candidate.key
		if committed {
			break
		}
	}
	pass.examined += examined
	complete := examined == len(window) && (len(window) < limit || (pass.hasAfter && pass.after.equal(pass.upper)))
	return pass, complete, nil
}

func (s *jobRunnerScheduler) recordLease() {
	s.mu.Lock()
	s.leasesSinceWrap++
	s.idle = false
	s.mu.Unlock()
}

// runCleanup is the scheduler's one cleanup worker. Each tick probes at most
// 128 pass entries and removes those whose tenant no longer has pending Job
// Runner work; it issues no SQL while the map is empty.
func (s *jobRunnerScheduler) runCleanup() {
	defer close(s.workerDone)
	ticker := time.NewTicker(jobRunnerCleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			_ = s.cleanupOnce(s.ctx)
		}
	}
}

type jobRunnerCleanupProbe struct {
	pass       *jobRunnerPass
	generation uint64
}

func (s *jobRunnerScheduler) cleanupOnce(ctx context.Context) error {
	s.mu.Lock()
	keys := make([]string, 0, len(s.passes))
	for key := range s.passes {
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		s.mu.Unlock()
		return nil
	}
	sort.Strings(keys)
	// Round-robin: continue after the last probed key, then wrap.
	start := sort.SearchStrings(keys, s.cleanupAfter)
	if start < len(keys) && keys[start] == s.cleanupAfter {
		start++
	}
	selected := make([]string, 0, min(len(keys), jobRunnerCleanupKeys))
	for offset := 0; offset < len(keys) && len(selected) < jobRunnerCleanupKeys; offset++ {
		selected = append(selected, keys[(start+offset)%len(keys)])
	}
	probes := make(map[string]jobRunnerCleanupProbe, len(selected))
	for _, key := range selected {
		pass := s.passes[key]
		probes[key] = jobRunnerCleanupProbe{pass: pass, generation: pass.generation}
	}
	s.cleanupAfter = selected[len(selected)-1]
	s.mu.Unlock()

	probeCtx, cancel := context.WithTimeout(ctx, jobRunnerCleanupTimeout)
	defer cancel()
	absent, err := s.store.jobRunnerAbsentTenants(probeCtx, selected)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, key := range absent {
		probe, ok := probes[key]
		current := s.passes[key]
		if !ok || current == nil || current != probe.pass || current.generation != probe.generation || current.busy {
			continue
		}
		delete(s.passes, key)
	}
	return nil
}

type jobRunnerLeaseOutcome int

const (
	jobRunnerLeaseCommitted jobRunnerLeaseOutcome = iota
	jobRunnerLeaseIneligible
	jobRunnerLeaseContended
	jobRunnerLeaseLockTimeout
	jobRunnerLeaseStatementTimeout
)

// errJobRunnerBudgetSpent stops a call whose remaining budget cannot begin
// another unit of database work; it is a budget stop, not a failure.
var errJobRunnerBudgetSpent = errors.New("queue: job runner call budget spent")

// jobRunnerTimeouts returns the local statement and lock limits for the next
// unit of database work, shortened by the remaining call budget. It reports
// false when the budget no longer permits beginning work.
func jobRunnerTimeouts(ctx context.Context, statementLimit time.Duration) (string, string, bool) {
	statement, lock := statementLimit, min(jobRunnerLockTimeout, statementLimit)
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline).Truncate(time.Millisecond)
		if remaining < time.Millisecond {
			return "", "", false
		}
		statement, lock = min(statement, remaining), min(lock, remaining)
	}
	return strconv.FormatInt(statement.Milliseconds(), 10), strconv.FormatInt(lock.Milliseconds(), 10), true
}

func (s *PostgreSQLQueueStore) withJobRunnerDiscoveryTx(ctx context.Context, operation string, statementLimit time.Duration, fn func(*dbconnect.Tx) error) error {
	statementTimeout, _, ok := jobRunnerTimeouts(ctx, statementLimit)
	if !ok {
		return errJobRunnerBudgetSpent
	}
	return s.client.WithTx(ctx, operation, &sql.TxOptions{ReadOnly: true}, func(tx *dbconnect.Tx) error {
		if _, err := tx.Exec(ctx,
			`SELECT set_config('tetral.queue_maintenance', 'true', true), set_config('statement_timeout', $1, true)`,
			statementTimeout,
		); err != nil {
			return err
		}
		return fn(tx)
	})
}

// jobRunnerSeekTenant returns the next workspace after last that holds pending
// Job Runner work, wrapping to the first such workspace when none follows.
// wrapped reports that the second, unbounded seek supplied the tenant.
func (s *PostgreSQLQueueStore) jobRunnerSeekTenant(ctx context.Context, last string) (string, bool, error) {
	var tenant string
	wrapped := false
	err := s.withJobRunnerDiscoveryTx(ctx, "queue.job_runner_tenant_seek", jobRunnerStatementTimeout, func(tx *dbconnect.Tx) error {
		tenant, wrapped = "", false
		if last != "" {
			err := tx.QueryRow(ctx, jobRunnerNextTenantSQL, last).Scan(&tenant)
			if err == nil || !dbconnect.IsNoRows(err) {
				return err
			}
			wrapped = true
		}
		err := tx.QueryRow(ctx, jobRunnerFirstTenantSQL).Scan(&tenant)
		if dbconnect.IsNoRows(err) {
			tenant, wrapped = "", false
			return nil
		}
		return err
	})
	if err != nil {
		return "", false, err
	}
	return tenant, wrapped, nil
}

// jobRunnerStartPass reads the tenant's highest raw key by reverse index seek
// together with database time. found is false when the tenant has no pending
// Job Runner row.
func (s *PostgreSQLQueueStore) jobRunnerStartPass(ctx context.Context, tenant string) (jobRunnerKey, time.Time, bool, error) {
	var upper jobRunnerKey
	var passNow time.Time
	found := false
	err := s.withJobRunnerDiscoveryTx(ctx, "queue.job_runner_pass_start", jobRunnerStatementTimeout, func(tx *dbconnect.Tx) error {
		err := tx.QueryRow(ctx, jobRunnerPassUpperSQL, tenant).Scan(
			&upper.negativePriority, &upper.availableAt, &upper.partitionKey, &upper.partitionSequence, &upper.id, &passNow,
		)
		if dbconnect.IsNoRows(err) {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		return nil
	})
	if err != nil {
		return jobRunnerKey{}, time.Time{}, false, err
	}
	return upper, passNow, found, nil
}

func (s *PostgreSQLQueueStore) jobRunnerWindow(ctx context.Context, tenant string, pass jobRunnerPass, limit int) ([]jobRunnerCandidate, error) {
	args := []any{
		tenant,
		pass.upper.negativePriority, pass.upper.availableAt, pass.upper.partitionKey, pass.upper.partitionSequence, pass.upper.id,
		limit,
	}
	if pass.hasAfter {
		args = append(args, pass.after.negativePriority, pass.after.availableAt, pass.after.partitionKey, pass.after.partitionSequence, pass.after.id)
	}
	var window []jobRunnerCandidate
	err := s.withJobRunnerDiscoveryTx(ctx, "queue.job_runner_window", jobRunnerStatementTimeout, func(tx *dbconnect.Tx) error {
		window = window[:0]
		rows, err := tx.Query(ctx, jobRunnerWindowSQL(pass.hasAfter), args...)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var candidate jobRunnerCandidate
			if err := rows.Scan(
				&candidate.kind, &candidate.causalSessionID,
				&candidate.key.negativePriority, &candidate.key.availableAt, &candidate.key.partitionKey,
				&candidate.key.partitionSequence, &candidate.key.id, &candidate.createdAt,
			); err != nil {
				return err
			}
			window = append(window, candidate)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return window, nil
}

// jobRunnerNextFutureJob seeks the earliest not-yet-due Job Runner job and
// returns its availability together with database time.
func (s *PostgreSQLQueueStore) jobRunnerNextFutureJob(ctx context.Context) (time.Time, time.Time, bool, error) {
	var availableAt, now time.Time
	found := false
	err := s.withJobRunnerDiscoveryTx(ctx, "queue.job_runner_future_hint", jobRunnerStatementTimeout, func(tx *dbconnect.Tx) error {
		err := tx.QueryRow(ctx, jobRunnerNextFutureSQL).Scan(&availableAt, &now)
		if dbconnect.IsNoRows(err) {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		return nil
	})
	return availableAt, now, found, err
}

func (s *PostgreSQLQueueStore) jobRunnerAbsentTenants(ctx context.Context, tenants []string) ([]string, error) {
	if len(tenants) == 0 {
		return nil, nil
	}
	args := make([]any, len(tenants))
	for index, tenant := range tenants {
		args[index] = tenant
	}
	var absent []string
	err := s.withJobRunnerDiscoveryTx(ctx, "queue.job_runner_pass_cleanup", jobRunnerCleanupTimeout, func(tx *dbconnect.Tx) error {
		absent = absent[:0]
		rows, err := tx.Query(ctx, jobRunnerAbsentTenantsSQL(len(tenants)), args...)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var tenant string
			if err := rows.Scan(&tenant); err != nil {
				return err
			}
			absent = append(absent, tenant)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return absent, nil
}

// leaseJobRunnerCandidate attempts one exact lease in a fresh workspace
// transaction: Session arbitration by try-lock, the exact Queue row by SKIP
// LOCKED, then the shared eligibility predicate and token mint against
// clock_timestamp(). A busy owner, a missing row, an ineligible candidate or a
// local timeout leaves no mutation and lets the caller advance; any other
// failure stops the call.
func (s *PostgreSQLQueueStore) leaseJobRunnerCandidate(ctx context.Context, tenant string, candidate jobRunnerCandidate, request LeaseJobRunnerJobsRequest) (*Job, jobRunnerLeaseOutcome, error) {
	statementTimeout, lockTimeout, ok := jobRunnerTimeouts(ctx, jobRunnerStatementTimeout)
	if !ok {
		return nil, jobRunnerLeaseIneligible, errJobRunnerBudgetSpent
	}
	leaseToken, err := NewLeaseToken()
	if err != nil {
		return nil, jobRunnerLeaseIneligible, err
	}
	var job *Job
	outcome := jobRunnerLeaseIneligible
	err = s.client.WithWorkspaceTx(ctx, tenant, "queue.lease_job_runner_job", func(tx *dbconnect.Tx) error {
		if _, err := tx.Exec(ctx,
			`SELECT set_config('statement_timeout', $1, true), set_config('lock_timeout', $2, true)`,
			statementTimeout, lockTimeout,
		); err != nil {
			return err
		}
		if candidate.causalSessionID != "" {
			resource, err := storage.SessionRuntimeMutationAdvisoryLockResource(tenant, candidate.causalSessionID)
			if err != nil {
				return err
			}
			var locked bool
			if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1, $2)`,
				storage.SessionRuntimeMutationAdvisoryLockCategory, resource,
			).Scan(&locked); err != nil {
				return err
			}
			if !locked {
				outcome = jobRunnerLeaseContended
				return nil
			}
		}
		var lockedID string
		err := tx.QueryRow(ctx,
			`SELECT id FROM queue_jobs WHERE workspace_id = $1 AND id = $2 FOR UPDATE SKIP LOCKED`,
			tenant, candidate.key.id,
		).Scan(&lockedID)
		if dbconnect.IsNoRows(err) {
			outcome = jobRunnerLeaseContended
			return nil
		}
		if err != nil {
			return err
		}
		leased, err := scanJob(tx.QueryRow(ctx,
			`UPDATE queue_jobs candidate
			    SET status = 'leased',
			        leased_by = $3,
			        lease_token = $4,
			        leased_at = clock_timestamp(),
			        leased_until = clock_timestamp() + ($5::bigint * interval '1 millisecond'),
			        attempt_count = `+leaseAttemptCountExpression("$6")+`,
			        lease_previous_attempt_count = candidate.attempt_count,
			        updated_at = clock_timestamp()
			  WHERE candidate.workspace_id = $1
			    AND candidate.id = $2
			    AND candidate.kind IN (`+jobRunnerKindSQLList+`)
			    AND `+leaseEligibilityPredicate("clock_timestamp()")+`
			  RETURNING `+queueJobColumns,
			tenant, candidate.key.id, request.LeaseOwner, leaseToken,
			request.LeaseDuration.Milliseconds(), s.retryPolicy.MaxAttempts,
		))
		if dbconnect.IsNoRows(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if leased.MaxAttempts == 0 {
			leased.MaxAttempts = s.retryPolicy.MaxAttempts
		}
		job, outcome = leased, jobRunnerLeaseCommitted
		return nil
	})
	if err != nil {
		if ctx.Err() != nil {
			return nil, jobRunnerLeaseIneligible, ctx.Err()
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			switch pgErr.Code {
			case "55P03":
				return nil, jobRunnerLeaseLockTimeout, nil
			case "57014":
				return nil, jobRunnerLeaseStatementTimeout, nil
			}
		}
		return nil, jobRunnerLeaseIneligible, err
	}
	return job, outcome, nil
}
