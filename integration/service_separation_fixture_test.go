package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/mcpmanifest"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
)

// Job Runner and Bridge each run on an independent pool authenticated as their
// installed production role, so a missing or over-broad grant on either side of
// the separation fails these compositions. Queue, public event admission and
// Sandbox execution are not the owners under test; their stores keep the shared
// restricted test role (no superuser or BYPASSRLS). The admin pool only supplies
// initial state, fault injection and an independent SQL oracle.
type separatedOwners struct {
	ctx                       context.Context
	admin, runnerDB, bridgeDB *sql.DB
	peerDB                    *sql.DB
	runner                    *jobrunner.PostgreSQLRuntimeDeliveryStore
	bridge                    *agentruntimebridge.PostgreSQLBridgeAPIStore
	bridgeTrace               *bridgeExecutionQueryTracer
	queue                     *queue.PostgreSQLQueueStore
	sessionID, threadID       string
}

func newSeparatedOwners(t *testing.T, suffix string, mcp bool) *separatedOwners {
	t.Helper()
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Fatal("separated-owner acceptance requires managed PostgreSQL; missing TETRAL_TEST_DATABASE_URL")
	}
	peerDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	installed := storagetest.OpenWorkloadDB(t, admin, "job_runner")
	bridgeTrace := &bridgeExecutionQueryTracer{}
	bridgeDB := installed.OpenWorkload(t, "bridge", bridgeTrace)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	f := &separatedOwners{ctx: ctx, admin: admin, runnerDB: installed.DB, bridgeDB: bridgeDB, peerDB: peerDB, bridgeTrace: bridgeTrace, sessionID: "sesn_separated_" + suffix, threadID: "thr_separated_" + suffix}
	if mcp {
		seedMCPFamilySession(t, admin, f.sessionID, f.threadID, "claude")
	} else {
		sessionfixture.SeedBridgeAPISession(t, admin, "default", f.sessionID, f.threadID)
	}
	if _, err := admin.ExecContext(ctx, `INSERT INTO session_runtime_status(workspace_id,session_id,status,created_at,updated_at) VALUES('default',$1,'idle',clock_timestamp(),clock_timestamp())`, f.sessionID); err != nil {
		t.Fatalf("seed Session runtime-status invariant: %v", err)
	}
	runnerClient := dbconnect.NewClientForTesting(installed.DB)
	bridgeClient := dbconnect.NewClientForTesting(bridgeDB)
	// Bridge alone registers and promotes Runtime processes.
	seedFixtureRuntimeProcess(t, bridgeClient, "tetral-agent-runtime", "pod_separation")
	f.runner = jobrunner.NewJobRunnerRuntimeDeliveryStore(runnerClient, nil, jobrunner.JobRunnerConfig{AgentRuntimeGRPCPort: 9090}, func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{Namespace: "tetral-agent-runtime", PodName: "runtime-separation", PodUID: "pod_separation", PodIP: "127.0.0.1"}})
	})
	installFixtureRuntimeLoad(t, f.runner)
	f.bridge = agentruntimebridge.NewPostgreSQLBridgeAPIStore(bridgeClient)
	f.bridge.RuntimeBindingTokenHMACKey = []byte("test-only-separation-token-signing-key")
	f.queue = queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(peerDB))
	roles := map[string]string{}
	for owner, db := range map[string]*sql.DB{"job_runner": installed.DB, "bridge": bridgeDB} {
		var role string
		var super, bypass bool
		if err := db.QueryRowContext(ctx, `SELECT current_user, rolsuper, rolbypassrls FROM pg_roles WHERE rolname=current_user`).Scan(&role, &super, &bypass); err != nil || super || bypass {
			t.Fatalf("restricted %s owner role=%q super=%t bypass=%t err=%v", owner, role, super, bypass, err)
		}
		roles[owner] = role
	}
	if installed.DB == bridgeDB || roles["job_runner"] == roles["bridge"] {
		t.Fatal("owner pools must be distinct and authenticate as distinct production roles")
	}
	t.Logf("independent owner pools authenticate as installed job_runner and bridge roles; watchdog=60s")
	return f
}

func (f *separatedOwners) configJob(generation string) jobrunner.RuntimeJob {
	return jobrunner.RuntimeJob{Kind: queue.KindRuntimeConfigUpdate, WorkspaceID: "default", SessionID: f.sessionID, SessionThreadID: f.threadID, ConfigGeneration: generation, RuntimeInputID: "runtime_config_update:" + f.sessionID + ":" + generation}
}

func (f *separatedOwners) declare(t *testing.T) *bridgev1.RuntimeScope {
	t.Helper()
	plan, err := f.runner.PrepareRuntimeCommand(f.ctx, f.configJob("1"))
	if err != nil || plan.RuntimeConfig == nil || plan.AttemptedBinding.BindingID == "" {
		t.Fatalf("Runner binding declaration: %#v/%v", plan, err)
	}
	return observedAttemptScope(f.configJob("1"), plan.AttemptedBinding)
}

func (f *separatedOwners) cold(t *testing.T, scope *bridgev1.RuntimeScope) map[string]any {
	t.Helper()
	response, err := f.bridge.LoadContext(f.ctx, &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil || response.GetRuntimeBindingToken() == "" {
		t.Fatalf("Bridge cold load/token: %v/%v", response, err)
	}
	var value map[string]any
	if err := json.Unmarshal([]byte(response.GetContextJson()), &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func (f *separatedOwners) worker(sender jobrunner.RuntimeCommandSender) *jobrunner.JobRunner {
	return &jobrunner.JobRunner{Queue: tetralqueue.NewServer(f.queue, nil), Deliverer: jobrunner.RuntimePodDirectDeliverer{Store: f.runner, Sender: sender}, Config: jobrunner.JobRunnerConfig{LeaseOwner: "separated-owners", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour}}
}

func separatedJSON(t *testing.T, raw string) any {
	t.Helper()
	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		t.Fatal(err)
	}
	return value
}
func separatedEqual(t *testing.T, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		actual, _ := json.Marshal(got)
		expected, _ := json.Marshal(want)
		t.Fatalf("wire/state = %s; want literal %s", actual, expected)
	}
}

type separatedManifestGate struct {
	mu      sync.Mutex
	result  mcpmanifest.ListResult
	err     error
	entered chan mcpmanifest.ListRequest
	release chan struct{}
}

func (l *separatedManifestGate) ListMCPTools(ctx context.Context, request mcpmanifest.ListRequest) (mcpmanifest.ListResult, error) {
	if l.entered != nil {
		select {
		case l.entered <- request:
		case <-ctx.Done():
			return mcpmanifest.ListResult{}, ctx.Err()
		}
		select {
		case <-l.release:
		case <-ctx.Done():
			return mcpmanifest.ListResult{}, ctx.Err()
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.result, l.err
}

// The receiver records production wire commands; it makes no claim about the
// TypeScript Runtime catalog. Its accepted reply is only a transport fixture.
func separatedSender() *recordingRuntimeCommandSender {
	return &recordingRuntimeCommandSender{result: jobrunner.RuntimeDeliveryResult{Status: jobrunner.RuntimeDeliveryAccepted}}
}

func separatedWait[T any](ctx context.Context, t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-ctx.Done():
		t.Fatalf("bounded owner operation: %v", ctx.Err())
		var zero T
		return zero
	}
}

func (f *separatedOwners) sql(t *testing.T, query string, args ...any) {
	t.Helper()
	if _, err := f.admin.ExecContext(f.ctx, query, args...); err != nil {
		t.Fatalf("admin fixture/fault SQL: %v", err)
	}
}

func (f *separatedOwners) count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var count int
	if err := f.admin.QueryRowContext(f.ctx, query, args...).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func (f *separatedOwners) input(t *testing.T) (jobrunner.RuntimeJob, *queue.Job) {
	t.Helper()
	service := sessionevent.NewService(sessionevent.NewPostgreSQLStore(dbconnect.NewClientForTesting(f.peerDB)))
	if _, err := service.AppendClientEvents(f.ctx, workspace.DefaultID, f.sessionID, "separated-input", sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{Type: sessionevent.EventTypeUserMessage, Content: []sessionevent.ContentBlock{{Type: sessionevent.ContentBlockTypeText, Text: "exercise separated owners"}}}}}); err != nil {
		t.Fatal(err)
	}
	leased, err := f.queue.Lease(f.ctx, queue.LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "separation-input", MaxJobs: 1, LeaseDuration: time.Minute})
	if err != nil || len(leased) != 1 {
		t.Fatalf("actual input custody lease=%v/%v", leased, err)
	}
	var payload struct {
		WorkspaceID string   `json:"workspace_id"`
		SessionID   string   `json:"session_id"`
		ThreadID    string   `json:"session_thread_id"`
		InputID     string   `json:"runtime_input_id"`
		Events      []string `json:"event_ids"`
		From        int64    `json:"sequence_from"`
		To          int64    `json:"sequence_to"`
		Kind        string   `json:"input_kind"`
	}
	row := leased[0]
	if err := json.Unmarshal(row.PayloadJSON, &payload); err != nil {
		t.Fatal(err)
	}
	return jobrunner.RuntimeJob{JobID: row.ID, LeaseToken: row.LeaseToken, Kind: row.Kind, PartitionKey: row.PartitionKey, DedupeKey: row.DedupeKey, WorkspaceID: payload.WorkspaceID, SessionID: payload.SessionID, SessionThreadID: payload.ThreadID, RuntimeInputID: payload.InputID, EventIDs: payload.Events, SequenceFrom: payload.From, SequenceTo: payload.To, InputKind: payload.Kind, PayloadJSON: string(row.PayloadJSON), AttemptCount: int32(row.AttemptCount), MaxAttempts: int32(row.MaxAttempts)}, row
}

// jobRunnerFixtureEmptyAcquisitions bounds consecutive empty production
// acquisitions. Queue keeps each tenant's pass across calls, and a call that
// ends a retained pass returns no job even when work admitted or made due after
// that pass began is ready; the next call starts a fresh pass. Two consecutive
// empty acquisitions therefore mean no work is dispatchable, and no fixture
// waits between them except for the backoff after a failed acquisition. A
// failed acquisition does not change the count. It can start the pass that the
// next call continues; that pass covers the work admitted and due before the
// failed call, so it still holds the work a fixture made ready before
// acquiring. A fixture in which another owner can hold a candidate's
// Session lock waits for that owner before acquiring, because Queue skips a
// busy Session instead of waiting for it. It waits until the owner's last
// Session-locking write before the acquisition is visible: a dispatched job's
// acknowledgement; a Runtime FinishIdle's output-capture record when the
// fixture does not run that capture, or its adoption when it does. When the
// owner keeps calling the Bridge until the acquired job settles, it acquires
// behind a closed bridgeAdmissionGate instead.
const jobRunnerFixtureEmptyAcquisitions = 2

// acquireAndJoinJobRunner performs production Job Runner acquisitions until one
// dispatches work or two consecutive acquisitions are empty, then performs the
// production join of the dispatched jobs.
func acquireAndJoinJobRunner(ctx context.Context, runner *jobrunner.JobRunner) error {
	_, err := acquireAndJoinJobRunnerActive(ctx, runner)
	return err
}

// acquireAndJoinJobRunnerActive is acquireAndJoinJobRunner reporting whether
// any acquisition dispatched a job.
func acquireAndJoinJobRunnerActive(ctx context.Context, runner *jobrunner.JobRunner) (bool, error) {
	dispatched, err := acquireAndJoinJobRunnerJobs(ctx, runner, 1, nil)
	return dispatched > 0, err
}

// acquireAndJoinJobRunnerWorker is acquireAndJoinJobRunnerActive for a
// long-running worker. Like the production loop, it retries an Internal
// acquisition failure until ctx ends, logging each failure to t, instead of
// returning it after the last backoff step. It still returns any other
// acquisition error and every job error from the join.
func acquireAndJoinJobRunnerWorker(ctx context.Context, t *testing.T, runner *jobrunner.JobRunner) (bool, error) {
	dispatched, err := acquireAndJoinJobRunnerJobs(ctx, runner, 1, t.Logf)
	return dispatched > 0, err
}

// acquireAndJoinJobRunnerJobs performs acquireJobRunnerJobsRetrying, then joins
// every dispatched job. A job error from the join is wrapped so that a log
// tells it apart from an acquisition error.
func acquireAndJoinJobRunnerJobs(ctx context.Context, runner *jobrunner.JobRunner, want int, retryLogf func(format string, args ...any)) (int, error) {
	dispatched, acquireErr := acquireJobRunnerJobsRetrying(ctx, runner, want, retryLogf)
	joinErr := runner.JoinDispatched(ctx)
	if joinErr != nil {
		joinErr = fmt.Errorf("join dispatched Job Runner jobs: %w", joinErr)
	}
	return dispatched, errors.Join(acquireErr, joinErr)
}

// jobRunnerFixtureAcquisitionBackoff is the spacing of
// jobRunnerAcquisitionBackoff (services/job-runner), after which the production
// loop retries every failed acquisition, repeating the 1 s step until its
// context ends. Queue fails a direct lease call with Internal when a discovery
// statement exceeds its 40 ms statement timeout and no lease committed.
// acquireJobRunnerJobs retries only an Internal failure with this spacing and
// returns it after one more failure past the 1 s step;
// acquireAndJoinJobRunnerWorker repeats the 1 s step instead.
var jobRunnerFixtureAcquisitionBackoff = []time.Duration{
	100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, time.Second,
}

// acquireJobRunnerJobs performs production acquisitions until they have
// dispatched want jobs or two consecutive acquisitions are empty, without
// joining, so dispatched jobs run concurrently in their slots; Queue leases at
// most one job per workspace in one call. A failed acquisition dispatches
// nothing and is not empty. One that failed with Internal is retried after
// jobRunnerFixtureAcquisitionBackoff. Any other failure is returned at once,
// such as a setup error, a closed acquisition, or Unavailable from a draining
// Queue or from a lost TCP response after which a lease may have committed. A
// returned failure is wrapped as an acquisition error, joined with ctx.Err()
// when ctx has ended.
func acquireJobRunnerJobs(ctx context.Context, runner *jobrunner.JobRunner, want int) (int, error) {
	return acquireJobRunnerJobsRetrying(ctx, runner, want, nil)
}

// acquireJobRunnerJobsRetrying is acquireJobRunnerJobs. A non-nil retryLogf, a
// long-running worker's, lifts the bound on retrying Internal failures: they
// are retried until ctx ends, repeating the 1 s step as the production loop
// does, and each is logged through retryLogf.
func acquireJobRunnerJobsRetrying(ctx context.Context, runner *jobrunner.JobRunner, want int, retryLogf func(format string, args ...any)) (int, error) {
	dispatched, empty, failures := 0, 0, 0
	for dispatched < want && empty < jobRunnerFixtureEmptyAcquisitions {
		acquisition, err := runner.AcquireAndDispatch(ctx)
		dispatched += acquisition.Dispatched
		if err != nil {
			if ctx.Err() == nil && status.Code(err) == codes.Internal && (retryLogf != nil || failures < len(jobRunnerFixtureAcquisitionBackoff)) {
				delay := jobRunnerFixtureAcquisitionBackoff[min(failures, len(jobRunnerFixtureAcquisitionBackoff)-1)]
				if retryLogf != nil {
					retryLogf("Job Runner acquisition failed; retrying after %s: %v", delay, err)
				}
				retry := time.NewTimer(delay)
				select {
				case <-retry.C:
					failures++
					continue
				case <-ctx.Done():
					retry.Stop()
				}
			}
			if ctx.Err() != nil {
				err = errors.Join(err, ctx.Err())
			}
			return dispatched, fmt.Errorf("acquire Job Runner jobs: %w", err)
		}
		failures = 0
		if acquisition.Dispatched == 0 {
			empty++
		} else {
			empty = 0
		}
	}
	return dispatched, nil
}

// bridgeAdmissionGate holds a test Bridge server's unary calls at admission.
// close returns once every admitted call has returned, so no Bridge
// transaction holds a Session lock until open. A fixture closes it across an
// acquisition when its Runtime keeps calling the Bridge until the acquired job
// settles. It does not fit a Runtime whose admitted call waits for later
// fixture work, such as FinishIdle waiting for output capture: close would
// wait for that call.
type bridgeAdmissionGate struct {
	mu     sync.Mutex
	change *sync.Cond
	closed bool
	active int
}

func newBridgeAdmissionGate() *bridgeAdmissionGate {
	gate := &bridgeAdmissionGate{}
	gate.change = sync.NewCond(&gate.mu)
	return gate
}

func (g *bridgeAdmissionGate) unary(ctx context.Context, request any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	g.mu.Lock()
	for g.closed {
		g.change.Wait()
	}
	g.active++
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.active--
		g.change.Broadcast()
		g.mu.Unlock()
	}()
	return handler(ctx, request)
}

func (g *bridgeAdmissionGate) close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = true
	for g.active > 0 {
		g.change.Wait()
	}
}

func (g *bridgeAdmissionGate) open() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = false
	g.change.Broadcast()
}

// repairRuntimePodLoss performs one production Pod-loss repair run from a new
// discovery cycle and reports its repairs; failed candidates are an error.
func repairRuntimePodLoss(ctx context.Context, store *jobrunner.PostgreSQLRuntimeDeliveryStore) (int, error) {
	run := jobrunner.NewRuntimePodLossRepair(store, nil, nil).RepairRun(ctx)
	return run.Repaired, run.Err()
}

type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}
