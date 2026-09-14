package tetralsandbox

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	daytonaerrors "github.com/daytonaio/daytona/libs/sdk-go/pkg/errors"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/types"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestEnvironmentBuildProgressSurvivesFailureBudgetAndResumesActivation(t *testing.T) {
	h := newEnvironmentBuildHarness(t)
	for i := 0; i < queue.DefaultMaxAttempts+5; i++ {
		h.provider.state = []string{"pending", "building", "pulling"}[i%3]
		h.run()
		h.assertWaiting(i + 1)
		h.now = h.now.Add(queue.EnvironmentBuildPollInterval)
		// A new runner and builder carry no provider identity in memory.
		h.restart()
	}
	h.provider.state = "active"
	h.run()
	assertEnvironmentArtifactStatus(t, h.admin, "ws_execution_store", "env_execution_store", 1, "ready", "snapshot_build_test")
	h.assertQueueStatus(queue.StatusAcknowledged)
	if h.provider.creates != 1 {
		t.Fatalf("snapshot submissions = %d; want one", h.provider.creates)
	}

	fanout := &EnvironmentReadyFanoutJobRunner{Queue: h.queueServer, Store: h.artifacts, Config: h.runner.Config, Clock: func() time.Time { return h.now }}
	if err := fanout.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	var state, queueID string
	if err := h.admin.QueryRow(`SELECT state, queue_job_id FROM sandbox_lifecycle_operations WHERE kind='create'`).Scan(&state, &queueID); err != nil {
		t.Fatal(err)
	}
	if state != "pending" || queueID == "" {
		t.Fatalf("activation = %s/%s; want pending with a durable QUEUEJOB", state, queueID)
	}
	var executions string
	if err := h.admin.QueryRow(`SELECT execution_state FROM session_runtime_tool_results WHERE tool_use_event_id='evt_execution_a'`).Scan(&executions); err != nil {
		t.Fatal(err)
	}
	if executions != "waiting_activation" {
		t.Fatalf("tool execution = %s; want still waiting for activation, never a premature failure", executions)
	}
}

func TestEnvironmentBuildObservationErrorsDoNotBecomeInstallationFailure(t *testing.T) {
	h := newEnvironmentBuildHarness(t)
	h.run()
	h.provider.queryErr = context.DeadlineExceeded
	for i := 1; i <= queue.DefaultMaxAttempts+1; i++ {
		h.now = h.now.Add(queue.EnvironmentBuildPollInterval)
		h.run()
		h.assertWaiting(i + 1)
	}
	var stage string
	if err := h.admin.QueryRow(`SELECT failure_stage FROM environment_artifacts WHERE generation=1`).Scan(&stage); err != nil {
		t.Fatal(err)
	}
	if stage != "observe_artifact" {
		t.Fatalf("diagnostic stage = %s; want observation failure", stage)
	}
	h.provider.queryErr = nil
	h.provider.state = "active"
	h.now = h.now.Add(queue.EnvironmentBuildPollInterval)
	h.run()
	assertEnvironmentArtifactStatus(t, h.admin, "ws_execution_store", "env_execution_store", 1, "ready", "snapshot_build_test")
	if h.provider.creates != 1 {
		t.Fatal("query recovery created another snapshot")
	}
}

func TestEnvironmentBuildDeadlineAndWarningSurviveRestart(t *testing.T) {
	h := newEnvironmentBuildHarness(t)
	started := h.now
	h.run()
	h.now = started.Add(DefaultEnvironmentBuildWarnAfter)
	h.restart()
	// Changed process settings cannot renew a previously persisted deadline.
	h.runner.Config.BuildWarnAfter = time.Hour
	h.runner.Config.BuildTimeout = 2 * time.Hour
	h.run()
	h.assertWaiting(2)
	var deadline, warned time.Time
	if err := h.admin.QueryRow(`SELECT build_deadline_at, build_warned_at FROM environment_artifacts WHERE generation=1`).Scan(&deadline, &warned); err != nil {
		t.Fatal(err)
	}
	if !deadline.Equal(started.Add(DefaultEnvironmentBuildTimeout)) || !warned.Equal(h.now) {
		t.Fatalf("persisted timing = %s/%s", deadline, warned)
	}
	h.now = h.now.Add(queue.EnvironmentBuildPollInterval)
	h.run()
	if strings.Count(h.logs.String(), `"outcome":"waiting_overdue"`) != 1 {
		t.Fatalf("expected one overdue warning, logs: %s", h.logs.String())
	}
	queries := h.provider.queries
	h.now = deadline
	h.restart()
	h.run()
	h.assertTerminal("environment_build_wait_timeout")
	if h.provider.queries != queries {
		t.Fatal("expired build made another provider call")
	}
	// Late provider success never resurrects settled history.
	h.provider.state = "active"
	h.now = h.now.Add(time.Minute)
	h.run()
	h.assertTerminal("environment_build_wait_timeout")
}

func TestEnvironmentBuildExplicitProviderFailureSettlesWaiters(t *testing.T) {
	for _, state := range []string{"error", "build_failed"} {
		t.Run(state, func(t *testing.T) {
			h := newEnvironmentBuildHarness(t)
			h.run()
			h.provider.state = state
			h.now = h.now.Add(queue.EnvironmentBuildPollInterval)
			h.run()
			h.assertTerminal("environment_provider_build_failed")
			var providerState, ref string
			if err := h.admin.QueryRow(`SELECT provider_build_state, provider_build_ref FROM environment_artifacts WHERE generation=1`).Scan(&providerState, &ref); err != nil {
				t.Fatal(err)
			}
			if providerState != state || ref != h.provider.name {
				t.Fatal("provider failure lost its diagnosable state or reference")
			}
		})
	}
}

func TestEnvironmentBuildActiveResponseAfterDeadlineCannotActivate(t *testing.T) {
	h := newEnvironmentBuildHarness(t)
	deadline := h.now.Add(DefaultEnvironmentBuildTimeout)
	h.run()
	h.now = deadline.Add(-time.Second)
	h.provider.state = "active"
	h.provider.onGet = func() { h.now = deadline }
	h.run()
	h.assertTerminal("environment_build_wait_timeout")
	var count int
	if err := h.admin.QueryRow(`SELECT count(*) FROM queue_jobs WHERE kind='environment_ready_fanout'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("late response woke activation after the build deadline")
	}
}

func TestEnvironmentBuildReadyOnlyAdvancesSameInputGenerations(t *testing.T) {
	h := newEnvironmentBuildHarness(t)
	h.run()
	if _, err := h.admin.Exec(`INSERT INTO environment_artifacts
		(workspace_id, environment_id, generation, status, provider, normalized_config_hash, artifact_input_hash, runtime_network_policy_json, packages_json, created_at, updated_at)
		SELECT workspace_id, environment_id, n, 'pending', provider, normalized_config_hash,
		CASE WHEN n=2 THEN artifact_input_hash ELSE 'changed_packages' END, runtime_network_policy_json, packages_json, created_at, updated_at
		FROM environment_artifacts CROSS JOIN generate_series(2,3) n WHERE generation=1;
		UPDATE environments SET current_generation=3 WHERE id='env_execution_store'`); err != nil {
		t.Fatal(err)
	}
	h.provider.state = "active"
	h.now = h.now.Add(queue.EnvironmentBuildPollInterval)
	h.run()
	assertEnvironmentArtifactStatus(t, h.admin, "ws_execution_store", "env_execution_store", 2, "ready", "snapshot_build_test")
	assertEnvironmentArtifactStatus(t, h.admin, "ws_execution_store", "env_execution_store", 3, "pending", "")
	var current int
	if err := h.admin.QueryRow(`SELECT current_generation FROM environments WHERE id='env_execution_store'`).Scan(&current); err != nil {
		t.Fatal(err)
	}
	if current != 3 {
		t.Fatal("old build overwrote the current Environment generation")
	}
}

func TestEnvironmentBuildStoreErrorKeepsNotificationForFencedSettlement(t *testing.T) {
	h := newEnvironmentBuildHarness(t)
	h.provider.state = "active"
	if _, err := h.admin.Exec(`UPDATE queue_jobs SET attempt_count=9 WHERE id=$1`, h.jobID); err != nil {
		t.Fatal(err)
	}
	h.runner.Store = &rejectBuildReadyStore{EnvironmentBuildStore: h.artifacts}
	if err := h.runner.RunOnce(context.Background()); err == nil {
		t.Fatal("expected unavailable business store")
	}
	// Dead-lettering now would orphan an unfinished artifact and its waiters.
	h.assertQueueStatus(queue.StatusLeased)
	if _, err := h.admin.Exec(`UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second' WHERE id=$1`, h.jobID); err != nil {
		t.Fatal(err)
	}
	// Expiry is the production reclaim boundary, not an administrative status reset.
	client := h.artifacts.client
	if count, err := queue.NewPostgreSQLStore(client).ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{WorkspaceID: "ws_execution_store", Limit: 100}); err != nil || count != 1 {
		t.Fatalf("reclaim=%d, %v", count, err)
	}
	h.now = h.now.Add(time.Second)
	h.restart()
	h.run()
	h.assertTerminal("environment_build_attempts_exhausted")
}

type rejectBuildReadyStore struct{ EnvironmentBuildStore }

func (*rejectBuildReadyStore) MarkEnvironmentBuildReady(context.Context, EnvironmentBuildJob, string, time.Time) error {
	return errors.New("test business store unavailable")
}

func TestEnvironmentBuildDeferFencesPreviousLeaseAtSameAttempt(t *testing.T) {
	h := newEnvironmentBuildHarness(t)
	first := h.lease()
	firstCtx := withEnvironmentBuildQueueAuthority(context.Background(), first)
	if _, ok, err := h.artifacts.ClaimEnvironmentBuild(firstCtx, first, h.now); err != nil || !ok {
		t.Fatalf("first claim: %t, %v", ok, err)
	}
	if err := h.artifacts.MarkEnvironmentBuildWaiting(firstCtx, first, sandbox.BuildArtifactResult{State: sandbox.ArtifactBuildWaiting, ProviderState: "building"}, h.now); err != nil {
		t.Fatal(err)
	}
	if _, err := h.queueServer.Defer(firstCtx, &queuev1.DeferRequest{WorkspaceId: first.WorkspaceID, JobId: first.JobID, LeaseToken: first.LeaseToken}); err != nil {
		t.Fatal(err)
	}
	if jobs, err := h.queueServer.Lease(context.Background(), &queuev1.LeaseRequest{WorkspaceId: first.WorkspaceID, Kinds: []string{queue.KindEnvironmentBuild}, LeaseOwner: "early-worker", MaxJobs: 1, LeaseDurationMs: 60000}); err != nil || len(jobs.GetJobs()) != 0 {
		t.Fatalf("early lease = %v, %v", jobs, err)
	}
	h.now = h.now.Add(queue.EnvironmentBuildPollInterval)
	second := h.lease()
	if second.AttemptCount != first.AttemptCount || second.LeaseToken == first.LeaseToken {
		t.Fatal("deferral did not refund attempt and mint fresh custody")
	}
	secondCtx := withEnvironmentBuildQueueAuthority(context.Background(), second)
	if _, ok, err := h.artifacts.ClaimEnvironmentBuild(secondCtx, second, h.now); err != nil || !ok {
		t.Fatalf("successor claim: %t, %v", ok, err)
	}
	if err := h.artifacts.MarkEnvironmentBuildReady(firstCtx, first, "stale_snapshot", h.now); !errors.Is(err, errQueueLeaseLost) {
		t.Fatalf("stale writer = %v; want lease loss", err)
	}
	if response, err := h.queueServer.Defer(firstCtx, &queuev1.DeferRequest{WorkspaceId: first.WorkspaceID, JobId: first.JobID, LeaseToken: first.LeaseToken}); err != nil || response.GetUpdated() {
		t.Fatalf("stale deferral = %v, %v; want unchanged", response, err)
	}
	if err := h.artifacts.MarkEnvironmentBuildReady(secondCtx, second, "current_snapshot", h.now); err != nil {
		t.Fatal(err)
	}
	assertEnvironmentArtifactStatus(t, h.admin, first.WorkspaceID, first.EnvironmentID, 1, "ready", "current_snapshot")
}

// Only provider responses and scheduling time are controlled. Queue Lease,
// Defer, Ack, business transactions, and their live PostgreSQL lease fences run
// unchanged. Real lease time is intentionally not replaced by the test clock.
type environmentBuildHarness struct {
	t           *testing.T
	admin       *sql.DB
	now         time.Time
	jobID       string
	queueServer *tetralqueue.Server
	artifacts   *EnvironmentArtifactStore
	provider    *buildSnapshotResponses
	runner      *EnvironmentBuildJobRunner
	logs        bytes.Buffer
}

func newEnvironmentBuildHarness(t *testing.T) *environmentBuildHarness {
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	runtime := storagetest.OpenWorkloadDB(t, admin, "sandbox").DB
	seedSandboxExecutionStoreFixture(t, admin)
	if _, err := admin.Exec(`UPDATE environment_artifacts SET status='pending', provider_artifact_ref=NULL WHERE generation=1`); err != nil {
		t.Fatal(err)
	}
	client := dbconnect.NewClientForTesting(runtime)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(client, 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(sandboxTestQueueContext(t, runtime), work, ExecutionNeedsCreation); err != nil {
		t.Fatal(err)
	}
	h := &environmentBuildHarness{t: t, admin: admin, now: time.Now().UTC().Truncate(time.Microsecond), artifacts: NewEnvironmentArtifactStore(client), provider: &buildSnapshotResponses{state: "building"}}
	store := &environmentBuildQueueClock{PostgreSQLQueueStore: queue.NewPostgreSQLStore(client), now: func() time.Time { return h.now }}
	qj, err := store.Enqueue(context.Background(), queue.EnqueueRequest{
		WorkspaceID: "ws_execution_store", Kind: queue.KindEnvironmentBuild,
		PartitionKey: queue.FormatEnvironmentPartitionKey("ws_execution_store", "env_execution_store"),
		DedupeKey:    queue.FormatEnvironmentBuildDedupeKey("ws_execution_store", "env_execution_store", "1"),
		PayloadJSON:  []byte(`{"workspace_id":"ws_execution_store","environment_id":"env_execution_store","generation":"1"}`), Now: h.now,
	})
	if err != nil {
		t.Fatal(err)
	}
	h.jobID = qj.ID
	h.queueServer = tetralqueue.NewServer(store, slog.New(slog.NewJSONHandler(&h.logs, nil)))
	h.restart()
	return h
}

func (h *environmentBuildHarness) restart() {
	h.runner = &EnvironmentBuildJobRunner{
		Queue: h.queueServer, Store: h.artifacts,
		Providers: artifactProviderRegistry(h.t, &DaytonaAdapter{Artifacts: sandboxdriver.NewDaytonaArtifactBuilderForClient(h.provider, "test-image")}),
		Config:    EnvironmentRunnerConfig{WorkspaceID: "ws_execution_store", LeaseOwner: "build-test", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
		Clock:     func() time.Time { return h.now }, Logger: slog.New(slog.NewJSONHandler(&h.logs, nil)),
	}
}

func (h *environmentBuildHarness) run() {
	h.t.Helper()
	if err := h.runner.RunOnce(context.Background()); err != nil {
		h.t.Fatal(err)
	}
}

func (h *environmentBuildHarness) lease() EnvironmentBuildJob {
	h.t.Helper()
	resp, err := h.queueServer.Lease(context.Background(), &queuev1.LeaseRequest{WorkspaceId: "ws_execution_store", Kinds: []string{queue.KindEnvironmentBuild}, LeaseOwner: "build-test", MaxJobs: 1, LeaseDurationMs: 60000})
	if err != nil || len(resp.GetJobs()) != 1 {
		h.t.Fatalf("lease = %v, %v", resp, err)
	}
	job, err := DecodeEnvironmentBuildJob(resp.Jobs[0])
	if err != nil {
		h.t.Fatal(err)
	}
	return job
}

func (h *environmentBuildHarness) assertWaiting(wantDefers int) {
	h.t.Helper()
	var status string
	var attempts, defers int
	var available time.Time
	var noLease, waiting bool
	if err := h.admin.QueryRow(`SELECT status, attempt_count, defer_count, available_at, lease_token IS NULL AND leased_until IS NULL FROM queue_jobs WHERE id=$1`, h.jobID).Scan(&status, &attempts, &defers, &available, &noLease); err != nil {
		h.t.Fatal(err)
	}
	if status != "pending" || attempts != 0 || defers != wantDefers || !noLease || !available.Equal(h.now.Add(queue.EnvironmentBuildPollInterval)) {
		h.t.Fatalf("deferred QUEUEJOB: %s attempt=%d defer=%d available=%s released=%t", status, attempts, defers, available, noLease)
	}
	if err := h.admin.QueryRow(`SELECT a.status='pending' AND a.lease_token IS NULL AND o.state='waiting_artifact' AND r.execution_state='waiting_activation' AND r.result_json IS NULL FROM environment_artifacts a JOIN sandbox_lifecycle_operations o ON o.target_environment_generation=a.generation JOIN session_runtime_tool_results r ON r.waiting_activation_operation_id=o.operation_id WHERE r.tool_use_event_id='evt_execution_a'`).Scan(&waiting); err != nil {
		h.t.Fatal(err)
	}
	if !waiting {
		h.t.Fatal("normal waiting prematurely settled the artifact, activation, or tool")
	}
}

func (h *environmentBuildHarness) assertQueueStatus(want string) {
	h.t.Helper()
	var state string
	if err := h.admin.QueryRow(`SELECT status FROM queue_jobs WHERE id=$1`, h.jobID).Scan(&state); err != nil {
		h.t.Fatal(err)
	}
	if state != want {
		h.t.Fatalf("QUEUEJOB state=%s; want %s", state, want)
	}
}

func (h *environmentBuildHarness) assertTerminal(kind string) {
	h.t.Helper()
	h.assertQueueStatus(queue.StatusDeadLettered)
	var actual string
	var settled bool
	if err := h.admin.QueryRow(`SELECT last_error_kind FROM environment_artifacts WHERE status='failed' AND generation=1`).Scan(&actual); err != nil {
		h.t.Fatal(err)
	}
	if actual != kind {
		h.t.Fatalf("failure kind=%s; want %s", actual, kind)
	}
	if err := h.admin.QueryRow(`SELECT o.state='failed' AND r.result_json IS NOT NULL FROM sandbox_lifecycle_operations o JOIN session_runtime_tool_results r ON r.waiting_activation_operation_id=o.operation_id WHERE r.tool_use_event_id='evt_execution_a'`).Scan(&settled); err != nil {
		h.t.Fatal(err)
	}
	if !settled {
		h.t.Fatal("terminal build did not settle activation and its tool")
	}
}

type environmentBuildQueueClock struct {
	*queue.PostgreSQLQueueStore
	now func() time.Time
}

func (s *environmentBuildQueueClock) Lease(ctx context.Context, r queue.LeaseRequest) ([]*queue.Job, error) {
	r.Now = s.now()
	return s.PostgreSQLQueueStore.Lease(ctx, r)
}
func (s *environmentBuildQueueClock) Defer(ctx context.Context, r queue.DeferRequest) (bool, error) {
	r.Now = s.now()
	return s.PostgreSQLQueueStore.Defer(ctx, r)
}

type buildSnapshotResponses struct {
	state, name      string
	creates, queries int
	queryErr         error
	onGet            func()
}

func (p *buildSnapshotResponses) Get(_ context.Context, name string) (*types.Snapshot, error) {
	p.queries++
	if p.onGet != nil {
		p.onGet()
	}
	if p.queryErr != nil {
		return nil, p.queryErr
	}
	if p.name == "" {
		return nil, daytonaerrors.NewDaytonaNotFoundError("not found", http.Header{})
	}
	if name != p.name {
		return nil, errors.New("worker queried a different build")
	}
	return &types.Snapshot{ID: "snapshot_build_test", Name: p.name, State: p.state}, nil
}
func (p *buildSnapshotResponses) Create(_ context.Context, r *types.CreateSnapshotParams) (*types.Snapshot, <-chan string, error) {
	p.creates++
	p.name = r.Name
	return &types.Snapshot{ID: "snapshot_build_test", Name: p.name, State: p.state}, nil, nil
}
