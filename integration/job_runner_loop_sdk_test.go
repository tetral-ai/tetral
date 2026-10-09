package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/tetral-ai/tetral/internal/auth/authtest"
	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/environment"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

// loopDeliverer is the scenario's Runtime boundary. It reports every Runtime
// input it receives and holds it in its slot until the scenario releases that
// Session; other Runner kinds are accepted at once.
type loopDeliverer struct {
	mu        sync.Mutex
	release   map[string]chan struct{}
	delivered chan jobrunner.RuntimeJob
}

func (d *loopDeliverer) releaseChannel(sessionID string) chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	release := d.release[sessionID]
	if release == nil {
		release = make(chan struct{})
		d.release[sessionID] = release
	}
	return release
}

func (d *loopDeliverer) ReplayRuntimeDeliveryFinalization(context.Context, jobrunner.RuntimeJob) (jobrunner.RuntimeDeliveryResult, bool, error) {
	return jobrunner.RuntimeDeliveryResult{}, false, nil
}

func (d *loopDeliverer) DeliverRuntimeJob(ctx context.Context, job jobrunner.RuntimeJob) (jobrunner.RuntimeDeliveryResult, error) {
	if job.Kind != queue.KindRuntimeInput {
		return jobrunner.RuntimeDeliveryResult{Status: jobrunner.RuntimeDeliveryAccepted}, nil
	}
	d.delivered <- job
	select {
	case <-d.releaseChannel(job.SessionID):
		return jobrunner.RuntimeDeliveryResult{Status: jobrunner.RuntimeDeliveryAccepted}, nil
	case <-ctx.Done():
		return jobrunner.RuntimeDeliveryResult{}, ctx.Err()
	}
}

// loopLeaseControl observes LeaseJobRunnerJobs on the Runner's generated client.
// While notificationOnly is set, an empty response carries a one-hour hint, so
// only a notification or a slot completion can start the next acquisition.
// While hold is set, a response with jobs reaches the Runner only after the
// scenario resumes it.
type loopLeaseControl struct {
	notificationOnly atomic.Bool
	hold             atomic.Bool
	empty            chan struct{}
	held             chan []string
	resume           chan struct{}
}

func (c *loopLeaseControl) intercept(ctx context.Context, method string, request, reply any, conn *grpc.ClientConn, invoke grpc.UnaryInvoker, options ...grpc.CallOption) error {
	err := invoke(ctx, method, request, reply, conn, options...)
	response, ok := reply.(*queuev1.LeaseJobRunnerJobsResponse)
	if err != nil || method != queuev1.QueueService_LeaseJobRunnerJobs_FullMethodName || !ok {
		return err
	}
	if len(response.GetJobs()) == 0 {
		if c.notificationOnly.Load() {
			response.RetryAfterMs = int32(time.Hour / time.Millisecond)
		}
		select {
		case c.empty <- struct{}{}:
		default:
		}
		return nil
	}
	if c.hold.Load() {
		ids := make([]string, 0, len(response.GetJobs()))
		for _, job := range response.GetJobs() {
			ids = append(ids, job.GetId())
		}
		c.held <- ids
		<-c.resume
	}
	return nil
}

// repairDiscoveryTrace reports each repair run's cycle-start statement on the
// Job Runner role's pool.
type repairDiscoveryTrace struct{ runs chan struct{} }

type repairDiscoveryTraceKey struct{}

func (tr repairDiscoveryTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, repairDiscoveryTraceKey{}, data.SQL)
}

func (tr repairDiscoveryTrace) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	query, _ := ctx.Value(repairDiscoveryTraceKey{}).(string)
	if data.Err == nil && strings.Contains(query, "tetral_job_runner_binding_upper()") {
		tr.runs <- struct{}{}
	}
}

// The production Job Runner loop serves work admitted through the public SDK:
// RunJobRunnerLoop over the generated Queue client and a real Queue gRPC
// server, the Job Runner notification listener, and the repair owner.
func TestJobRunnerProductionLoopServesSDKWorkAcrossWorkspaces(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	// The SDK children run under ctx and must outlive the test body until their
	// own cleanup closes them.
	t.Cleanup(cancel)
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	pools := storagetest.OpenWorkloadDB(t, admin, "bridge")
	base, defaultKey := startContentSDKPublicEdge(t, pools, blob.NewFakeBlobStore())
	other := workspace.ID(id.New("workspace_"))
	if _, err := workspace.NewSeeder(admin).Seed(ctx, other, "runner loop other"); err != nil {
		t.Fatal(err)
	}
	otherKey, err := authtest.SeedIndependentKey(ctx, pools.OpenWorkload(t, "auth", nil), other, "runner-loop-other")
	if err != nil {
		t.Fatal(err)
	}
	environments := environment.NewPostgreSQLEnvironmentStore(dbconnect.NewClientForTesting(pools.OpenWorkload(t, "api", nil)), environment.WithDefaultArtifactRef("artifact_runner_loop"))
	sdk := map[workspace.ID]*contentSDKChild{
		workspace.DefaultID: startContentSDKChildContext(ctx, t, base, defaultKey),
		other:               startContentSDKChildContext(ctx, t, base, otherKey.APIKey),
	}
	environmentIDs := map[workspace.ID]string{}
	for ws := range sdk {
		created, err := environments.Create(ctx, ws, environment.CreateEnvironmentRequest{Name: "runner loop"})
		if err != nil {
			t.Fatal(err)
		}
		environmentIDs[ws] = created.ID
	}
	sessionWorkspace := map[string]workspace.ID{}
	provision := func(ws workspace.ID, name string) string {
		t.Helper()
		raw := sdk[ws].control(t, "provision", map[string]any{"environmentId": environmentIDs[ws], "agent": map[string]any{"name": name, "model": "anthropic/claude-opus-4-8", "approval_mode": "full_access", "tools": []any{map[string]any{"type": "tetral_agent_toolset", "family": "claude"}}, "skills": []any{}, "metadata": map[string]any{}}})
		var provisioned struct {
			Session struct {
				ID string `json:"id"`
			} `json:"session"`
		}
		if json.Unmarshal(raw, &provisioned) != nil || provisioned.Session.ID == "" {
			t.Fatalf("SDK provision of %s lacked a Session", name)
		}
		sessionWorkspace[provisioned.Session.ID] = ws
		return provisioned.Session.ID
	}
	a1, a2, a3 := provision(workspace.DefaultID, "a1"), provision(workspace.DefaultID, "a2"), provision(workspace.DefaultID, "a3")
	b1, b2 := provision(other, "b1"), provision(other, "b2")
	send := func(sessionID string) {
		t.Helper()
		sdk[sessionWorkspace[sessionID]].control(t, "send", map[string]any{"sessionId": sessionID, "text": "runner loop " + sessionID})
	}

	// Queue: the production store and gRPC service under the Queue role.
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(pools.OpenWorkload(t, "queue", nil)))
	queueStore.StartJobRunnerScheduler()
	t.Cleanup(queueStore.QuiesceJobRunnerScheduler)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	tetralqueue.Register(server, queueStore, slog.New(slog.NewTextHandler(io.Discard, nil)))
	served := make(chan struct{})
	go func() { defer close(served); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-served })
	control := &loopLeaseControl{empty: make(chan struct{}, 1), held: make(chan []string, 1), resume: make(chan struct{})}
	control.notificationOnly.Store(true)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(control.intercept))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// Job Runner: production loop, listener and repair owner wired as the
	// executable wires them, on the Job Runner role.
	repairRuns := make(chan struct{}, 32)
	runnerClient := dbconnect.NewClientForTesting(pools.OpenWorkload(t, "job_runner", repairDiscoveryTrace{runs: repairRuns}))
	repairStore := jobrunner.NewPostgreSQLRuntimeDeliveryStore(runnerClient, 9090, jobrunner.KubernetesRuntimeTargetResolver{Snapshot: func() kubernetes.BindingVisibilitySnapshot {
		return kubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	}})
	repairSignal := jobrunner.NewRuntimePodLossRepairSignal()
	repairRequests := make(chan struct{}, 8)
	deliverer := &loopDeliverer{release: map[string]chan struct{}{}, delivered: make(chan jobrunner.RuntimeJob, 8)}
	wake := queue.NewWakeSignal()
	acquisition, closeAcquisition := context.WithCancel(ctx)
	defer closeAcquisition()
	var loopErr error
	loopDone := make(chan struct{})
	listenerDone := make(chan struct{})
	go func() {
		defer close(listenerDone)
		_ = queue.RunNotificationListenerWithSignals(acquisition, queue.PostgreSQLNotificationListener{Client: runnerClient}, queue.ConsumerClassJobRunner, wake,
			map[string]func(){queue.NotificationClassRuntimeProcess: func() {
				repairRequests <- struct{}{}
				repairSignal.Mark()
			}}, nil)
	}()
	go func() {
		defer close(loopDone)
		loopErr = jobrunner.RunJobRunnerLoop(acquisition, &jobrunner.JobRunner{
			Queue:     jobrunner.QueueClientFromGRPC(queuev1.NewQueueServiceClient(conn)),
			Deliverer: deliverer,
			Repair:    jobrunner.NewRuntimePodLossRepair(repairStore, repairSignal, nil),
			Config:    jobrunner.JobRunnerConfig{LeaseOwner: "runner-loop-sdk", MaxJobs: 2, LeaseDuration: 30 * time.Second, HeartbeatInterval: 10 * time.Second, DrainTimeout: 5 * time.Second, CancelJoinTimeout: 5 * time.Second},
		}, nil, wake)
	}()
	t.Cleanup(func() {
		closeAcquisition()
		<-listenerDone
		<-loopDone
	})

	// Each step has its own failure bound so a missing wake fails at that
	// step. A repair wake is bounded well below the 30 s periodic repair
	// deadline, so only the signal path can satisfy it.
	const (
		stepBound       = 30 * time.Second
		repairWakeBound = 10 * time.Second
	)
	awaitWithin := func(what string, ready <-chan struct{}, bound time.Duration) {
		t.Helper()
		select {
		case <-ready:
		case <-time.After(bound):
			t.Fatalf("%s did not happen within %s", what, bound)
		}
	}
	await := func(what string, ready <-chan struct{}) {
		t.Helper()
		awaitWithin(what, ready, stepBound)
	}
	expectDelivery := func(sessionID string) jobrunner.RuntimeJob {
		t.Helper()
		select {
		case job := <-deliverer.delivered:
			if job.SessionID != sessionID || job.WorkspaceID != sessionWorkspace[sessionID].String() {
				t.Fatalf("delivered %s/%s; want %s/%s", job.WorkspaceID, job.SessionID, sessionWorkspace[sessionID], sessionID)
			}
			return job
		case <-time.After(stepBound):
			t.Fatalf("Runtime input of %s was not delivered", sessionID)
		}
		return jobrunner.RuntimeJob{}
	}
	jobStatus := func(jobID string) (status string, attempts int, leasedAt, availableAt sql.NullTime, token, previous sql.NullString) {
		t.Helper()
		if err := admin.QueryRowContext(ctx, `SELECT status, attempt_count, leased_at, available_at, lease_token, lease_previous_attempt_count::text
			FROM queue_jobs WHERE id=$1`, jobID).Scan(&status, &attempts, &leasedAt, &availableAt, &token, &previous); err != nil {
			t.Fatal(err)
		}
		return status, attempts, leasedAt, availableAt, token, previous
	}

	// The loop starts idle: once provisioning work, if any, has drained and an
	// empty response has installed the one-hour hint, only a wake can lease.
	for {
		var busy int
		if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM queue_jobs WHERE status IN ('pending','leased') AND kind IN ('runtime_input','runtime_recovery','runtime_config_update','cleanup_session','session_delete_cleanup')`).Scan(&busy); err != nil {
			t.Fatal(err)
		}
		await("an empty acquisition", control.empty)
		if busy == 0 {
			break
		}
	}

	// Committed notifications wake acquisition, and one Runner leases work from
	// both workspaces into its two slots.
	send(a1)
	expectDelivery(a1)
	send(b1)
	b1Job := expectDelivery(b1)

	// With every slot busy, a committed process-takeover notification still
	// starts a repair run.
	awaitWithin("the startup repair run", repairRuns, repairWakeBound)
	awaitWithin("the listener's catch-up repair request", repairRequests, repairWakeBound)
	awaitWithin("the catch-up repair run", repairRuns, repairWakeBound)
	if _, err := admin.ExecContext(ctx, `SELECT pg_notify($1, $2)`, queue.NotificationChannel, queue.NotificationClassRuntimeProcess); err != nil {
		t.Fatal(err)
	}
	awaitWithin("the process-takeover repair request", repairRequests, repairWakeBound)
	awaitWithin("the signalled repair run", repairRuns, repairWakeBound)

	// Work committed while both slots are busy waits; when a1 finishes, its slot
	// is refilled at once while b1 is still running.
	send(a2)
	close(deliverer.releaseChannel(a1))
	a2Job := expectDelivery(a2)
	if status, _, _, _, _, _ := jobStatus(b1Job.JobID); status != queue.StatusLeased {
		t.Fatalf("b1 job status at refill = %s; want still leased", status)
	}

	// Work that becomes due later is never announced: Queue's retry hint alone
	// brings the next acquisition once it is due.
	control.notificationOnly.Store(false)
	send(b2)
	updated, err := admin.ExecContext(ctx, `UPDATE queue_jobs SET available_at = clock_timestamp() + interval '1500 milliseconds'
		WHERE kind='runtime_input' AND status='pending' AND workspace_id=$1 AND payload_json::jsonb->>'session_id'=$2`, other.String(), b2)
	if err != nil {
		t.Fatal(err)
	}
	if rows, err := updated.RowsAffected(); err != nil || rows != 1 {
		t.Fatalf("defer b2 availability rows = %d/%v; want 1", rows, err)
	}
	close(deliverer.releaseChannel(a2))
	b2Job := expectDelivery(b2)
	if _, _, leasedAt, availableAt, _, _ := jobStatus(b2Job.JobID); !leasedAt.Valid || leasedAt.Time.Before(availableAt.Time) {
		t.Fatalf("b2 leased at %v before it was due at %v", leasedAt.Time, availableAt.Time)
	}
	if status, _, _, _, _, _ := jobStatus(a2Job.JobID); status != queue.StatusAcknowledged {
		t.Fatalf("a2 job status = %s; want acknowledged", status)
	}

	// Quiesce while a lease response is in flight: the observed lease is not
	// dispatched and returns to pending with its attempt count restored.
	close(deliverer.releaseChannel(b1))
	close(deliverer.releaseChannel(b2))
	for _, jobID := range []string{b1Job.JobID, b2Job.JobID} {
		for {
			if status, _, _, _, _, _ := jobStatus(jobID); status == queue.StatusAcknowledged {
				break
			}
			await("a later acquisition", control.empty)
		}
	}
	control.hold.Store(true)
	send(a3)
	var held []string
	select {
	case held = <-control.held:
	case <-time.After(stepBound):
		t.Fatal("a3 lease response was not observed")
	}
	closeAcquisition()
	close(control.resume)
	await("the Runner loop join after quiesce", loopDone)
	if loopErr != nil {
		t.Fatalf("Runner loop = %v", loopErr)
	}
	select {
	case job := <-deliverer.delivered:
		t.Fatalf("delivered %s after acquisition closed", job.SessionID)
	default:
	}
	var heldSession string
	if len(held) == 1 {
		if err := admin.QueryRowContext(ctx, `SELECT payload_json::jsonb->>'session_id' FROM queue_jobs WHERE id=$1`, held[0]).Scan(&heldSession); err != nil {
			t.Fatal(err)
		}
	}
	if heldSession != a3 {
		t.Fatalf("held lease response jobs = %v (Session %q); want the a3 job only", held, heldSession)
	}
	if status, attempts, _, _, token, previous := jobStatus(held[0]); status != queue.StatusPending || attempts != 0 || token.Valid || previous.Valid {
		t.Fatalf("released a3 job = %s attempts=%d token=%t previous=%t; want pending, 0, no lease", status, attempts, token.Valid, previous.Valid)
	}
}
