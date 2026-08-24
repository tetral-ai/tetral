package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	tetralcleanup "github.com/tetral-ai/tetral/services/cleanup"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

type cleanupCompositionProcess struct {
	command    *exec.Cmd
	output     bytes.Buffer
	port       int
	effectPath string
	closePath  string
}

func TestPostgreSQLDelayedBusyCleanupReschedulesWithoutSpendingRetry(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_clean_busy"
	const threadID = "thrd_clean_busy"
	const cleanupID = "cleanup_busy"
	process := startCleanupComposition(t, "delayed_busy", "pod_uid_"+sessionID)
	queueStore, deliveryStore, runner, queueJobID := seedCleanupComposition(
		t, runtimeDB, admin, process.port, sessionID, threadID, cleanupID, 2,
	)
	_ = queueStore
	_ = deliveryStore

	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("run delayed busy cleanup = active:%t err:%v", active, err)
	}
	var queueStatus string
	var attempts int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$1`, queueJobID,
	).Scan(&queueStatus, &attempts); err != nil {
		t.Fatalf("read delayed busy Queue state: %v", err)
	}
	if queueStatus != queue.StatusAcknowledged || attempts != 1 {
		t.Fatalf("delayed busy Queue state = %s/%d; want acknowledged/1", queueStatus, attempts)
	}
	assertCleanupMarkersRearmed(t, admin, sessionID, true)
}

func TestPostgreSQLSuccessfulCleanupCompletesHostBeforeDurableFinalization(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_cleanup_success"
	const threadID = "thrd_cleanup_success"
	const cleanupID = "cleanup_success"
	process := startCleanupComposition(t, "success", "pod_uid_"+sessionID)
	_, _, runner, queueJobID := seedCleanupComposition(
		t, runtimeDB, admin, process.port, sessionID, threadID, cleanupID, 2,
	)

	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("run successful cleanup = active:%t err:%v", active, err)
	}
	if _, err := os.Stat(process.effectPath); err != nil {
		t.Fatalf("Runtime cleanup effect was not completed before Bridge finalization: %v", err)
	}
	var queueStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$1`, queueJobID,
	).Scan(&queueStatus); err != nil {
		t.Fatalf("read successful cleanup Queue state: %v", err)
	}
	if queueStatus != queue.StatusAcknowledged {
		t.Fatalf("successful cleanup Queue state = %s; want acknowledged", queueStatus)
	}
	var bindings int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1`, sessionID,
	).Scan(&bindings); err != nil {
		t.Fatalf("read successful cleanup binding: %v", err)
	}
	if bindings != 0 {
		t.Fatalf("successful cleanup retained %d Runtime bindings; want zero", bindings)
	}
}

func TestPostgreSQLFinalAttemptCleanupResponseLossReplaysSameHostOutcome(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_clean_final_loss"
	const cleanupID = "clean_loss"
	process := startCleanupComposition(t, "success", "pod_uid_"+sessionID)
	_, _, runner, queueJobID := seedCleanupComposition(
		t, runtimeDB, admin, process.port, sessionID, "thrd_clean_final_loss", cleanupID, 1,
	)
	lost := &cleanupLostResponseSender{
		RuntimeCommandSender: NewRuntimePodCommandClient(taskNotificationRuntimeTokenSource{}),
	}
	runner.Deliverer = RuntimePodDirectDeliverer{
		Store:  runner.Deliverer.(RuntimePodDirectDeliverer).Store,
		Sender: lost,
	}

	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("run final cleanup with lost response = active:%t err:%v", active, err)
	}
	var status string
	var attempts int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$1`, queueJobID,
	).Scan(&status, &attempts); err != nil {
		t.Fatalf("read outcome-unknown cleanup Queue state: %v", err)
	}
	if status != queue.StatusPending || attempts != 1 {
		t.Fatalf("outcome-unknown cleanup Queue state = %s/%d; want pending/1", status, attempts)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE queue_jobs SET available_at=clock_timestamp()-interval '1 second' WHERE workspace_id='default' AND id=$1`, queueJobID,
	); err != nil {
		t.Fatalf("make cleanup replay ready: %v", err)
	}

	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("replay final cleanup outcome = active:%t err:%v", active, err)
	}
	var bindings int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$1`, queueJobID,
	).Scan(&status, &attempts); err != nil {
		t.Fatalf("read replayed cleanup Queue state: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1`, sessionID,
	).Scan(&bindings); err != nil {
		t.Fatalf("read replayed cleanup binding: %v", err)
	}
	if status != queue.StatusAcknowledged || attempts != 1 || bindings != 0 {
		t.Fatalf("replayed cleanup state = %s/%d bindings=%d; want acknowledged/1/0", status, attempts, bindings)
	}
	var effect struct {
		HostInvocations int `json:"hostInvocations"`
		HostEffects     int `json:"hostEffects"`
	}
	raw, err := os.ReadFile(process.effectPath)
	if err != nil || json.Unmarshal(raw, &effect) != nil || effect.HostInvocations != 2 || effect.HostEffects != 1 {
		t.Fatalf("replayed cleanup host effect = %s/%v; want two calls and one real SessionManager removal", raw, err)
	}
}

func TestPostgreSQLCleanupExhaustionReleasesMarkerAndAllowsFreshSweep(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_clean_exhaust"
	const threadID = "thrd_clean_exhaust"
	const cleanupID = "cleanup_exhaust"
	process := startCleanupComposition(t, "failure", "pod_uid_"+sessionID)
	_, deliveryStore, runner, queueJobID := seedCleanupComposition(
		t, runtimeDB, admin, process.port, sessionID, threadID, cleanupID, 2,
	)
	responseLoss := &cleanupExhaustionResponseLossDeliverer{
		RuntimePodDirectDeliverer: runner.Deliverer.(RuntimePodDirectDeliverer),
	}
	runner.Deliverer = responseLoss

	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("run first failing cleanup = active:%t err:%v", active, err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE queue_jobs SET available_at=clock_timestamp()-interval '1 second' WHERE workspace_id='default' AND id=$1`, queueJobID,
	); err != nil {
		t.Fatalf("make final cleanup attempt ready: %v", err)
	}
	if active, err := runner.RunOnceWithActivity(context.Background()); err == nil || !active {
		t.Fatalf("run final cleanup with response loss = active:%t err:%v; want committed outcome with lost response", active, err)
	}
	var queueStatus, errorKind, errorMessage string
	var attempts int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, attempt_count, last_error_kind, last_error_message FROM queue_jobs WHERE workspace_id='default' AND id=$1`, queueJobID,
	).Scan(&queueStatus, &attempts, &errorKind, &errorMessage); err != nil {
		t.Fatalf("read exhausted cleanup Queue state: %v", err)
	}
	if queueStatus != queue.StatusDeadLettered || attempts != 2 || errorKind != "cleanup_failed" || errorMessage != "runtime rejected operation" {
		t.Fatalf("exhausted cleanup Queue state = %s/%d/%s/%s; want dead_lettered/2/cleanup_failed/runtime rejected operation", queueStatus, attempts, errorKind, errorMessage)
	}
	cleanupAfter := assertCleanupMarkersRearmed(t, admin, sessionID, true)
	var bindings int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1`, sessionID,
	).Scan(&bindings); err != nil {
		t.Fatalf("read exhausted cleanup binding: %v", err)
	}
	if bindings != 1 {
		t.Fatalf("exhausted cleanup bindings = %d; want retained binding", bindings)
	}

	replayed, err := deliveryStore.FinalizeRuntimeCleanupExhaustion(context.Background(), responseLoss.job, RuntimeDeliveryResult{})
	if err != nil || replayed.Status != RuntimeDeliveryDuplicate || !replayed.QueueLeaseSettled {
		t.Fatalf("replay exhausted cleanup = %#v/%v; want settled duplicate", replayed, err)
	}

	scheduler := tetralcleanup.NewScheduler(dbconnect.NewClientForTesting(runtimeDB))
	scheduler.Clock = func() time.Time { return cleanupAfter.Add(time.Second) }
	nextID := 0
	scheduler.IDStrategy = func(prefix string) string {
		nextID++
		return prefix + "next_" + time.Duration(nextID).String()
	}
	claimed, err := scheduler.ClaimDue(context.Background(), tetralcleanup.ClaimDueRequest{WorkspaceID: workspace.DefaultID, Limit: 1})
	if err != nil {
		t.Fatalf("claim cleanup successor: %v", err)
	}
	if len(claimed) != 1 || claimed[0].CleanupJobID == cleanupID || claimed[0].QueueJobID == queueJobID {
		t.Fatalf("cleanup successor = %#v; want one fresh identity", claimed)
	}
}

func TestPostgreSQLCleanupInvalidResponseRetriesThenReleasesMarkerAtExhaustion(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_clean_invresp"
	const threadID = "thrd_clean_invresp"
	const cleanupID = "cleanup_invresp"
	process := startCleanupComposition(t, "success", "pod_uid_"+sessionID)
	_, _, runner, queueJobID := seedCleanupComposition(
		t, runtimeDB, admin, process.port, sessionID, threadID, cleanupID, 2,
	)
	direct := runner.Deliverer.(RuntimePodDirectDeliverer)
	sender := &cleanupInvalidResponseSender{RuntimeCommandSender: direct.Sender}
	direct.Sender = sender
	runner.Deliverer = direct

	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("run non-final invalid cleanup response = active:%t err:%v", active, err)
	}
	var queueStatus, errorKind string
	var attempts int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, attempt_count, last_error_kind FROM queue_jobs WHERE workspace_id='default' AND id=$1`, queueJobID,
	).Scan(&queueStatus, &attempts, &errorKind); err != nil {
		t.Fatalf("read retried invalid cleanup Queue state: %v", err)
	}
	if queueStatus != queue.StatusPending || attempts != 1 || errorKind != "invalid_runtime_response" {
		t.Fatalf("retried invalid cleanup state = %s/%d/%s; want pending/1/invalid_runtime_response", queueStatus, attempts, errorKind)
	}
	assertCleanupMarkersClaimed(t, admin, sessionID, cleanupID)

	if _, err := admin.ExecContext(context.Background(),
		`UPDATE queue_jobs SET available_at=clock_timestamp()-interval '1 second' WHERE workspace_id='default' AND id=$1`, queueJobID,
	); err != nil {
		t.Fatalf("make final invalid cleanup attempt ready: %v", err)
	}
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("run final invalid cleanup response = active:%t err:%v", active, err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, attempt_count, last_error_kind FROM queue_jobs WHERE workspace_id='default' AND id=$1`, queueJobID,
	).Scan(&queueStatus, &attempts, &errorKind); err != nil {
		t.Fatalf("read exhausted invalid cleanup Queue state: %v", err)
	}
	if queueStatus != queue.StatusDeadLettered || attempts != 2 || errorKind != "invalid_runtime_response" {
		t.Fatalf("exhausted invalid cleanup state = %s/%d/%s; want dead_lettered/2/invalid_runtime_response", queueStatus, attempts, errorKind)
	}
	cleanupAfter := assertCleanupMarkersRearmed(t, admin, sessionID, true)
	if sender.calls.Load() != 2 {
		t.Fatalf("invalid cleanup Runtime calls = %d; want 2 bounded attempts", sender.calls.Load())
	}
	var effect struct {
		HostInvocations int `json:"hostInvocations"`
		HostEffects     int `json:"hostEffects"`
	}
	raw, err := os.ReadFile(process.effectPath)
	if err != nil || json.Unmarshal(raw, &effect) != nil || effect.HostInvocations != 2 || effect.HostEffects != 1 {
		t.Fatalf("invalid cleanup host effect = %s/%v; want two calls and one idempotent effect", raw, err)
	}

	scheduler := tetralcleanup.NewScheduler(dbconnect.NewClientForTesting(runtimeDB))
	scheduler.Clock = func() time.Time { return cleanupAfter.Add(time.Second) }
	scheduler.IDStrategy = func(prefix string) string { return prefix + "next" }
	claimed, err := scheduler.ClaimDue(context.Background(), tetralcleanup.ClaimDueRequest{WorkspaceID: workspace.DefaultID, Limit: 1})
	if err != nil {
		t.Fatalf("claim cleanup successor after invalid response exhaustion: %v", err)
	}
	if len(claimed) != 1 || claimed[0].CleanupJobID == cleanupID || claimed[0].QueueJobID == queueJobID {
		t.Fatalf("invalid response cleanup successor = %#v; want one fresh identity", claimed)
	}
}

func TestPostgreSQLCleanupExhaustionRollsBackQueueAndMarkerTogether(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_clean_rollback"
	const cleanupID = "cleanup_rollback"
	process := startCleanupComposition(t, "failure", "pod_uid_"+sessionID)
	_, deliveryStore, runner, queueJobID := seedCleanupComposition(
		t, runtimeDB, admin, process.port, sessionID, "thrd_clean_rollback", cleanupID, 2,
	)
	capture := &cleanupExhaustionResponseLossDeliverer{
		RuntimePodDirectDeliverer: runner.Deliverer.(RuntimePodDirectDeliverer),
		lost:                      true,
	}
	runner.Deliverer = capture

	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("run first rollback cleanup attempt = active:%t err:%v", active, err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE queue_jobs SET available_at=clock_timestamp()-interval '1 second' WHERE workspace_id='default' AND id=$1`, queueJobID,
	); err != nil {
		t.Fatalf("make rollback cleanup attempt ready: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `
		CREATE FUNCTION fail_cleanup_marker_update() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			RAISE EXCEPTION 'injected cleanup marker failure';
		END $$;
		CREATE TRIGGER fail_cleanup_marker_update
		BEFORE UPDATE OF cleanup_after, cleanup_job_id ON session_runtime_status
		FOR EACH ROW WHEN (OLD.session_id = 'sesn_clean_rollback')
		EXECUTE FUNCTION fail_cleanup_marker_update()`); err != nil {
		t.Fatalf("install cleanup rollback trigger: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.ExecContext(context.Background(), `DROP TRIGGER IF EXISTS fail_cleanup_marker_update ON session_runtime_status`)
		_, _ = admin.ExecContext(context.Background(), `DROP FUNCTION IF EXISTS fail_cleanup_marker_update()`)
	})

	if active, err := runner.RunOnceWithActivity(context.Background()); err == nil || !active {
		t.Fatalf("run cleanup with injected transaction failure = active:%t err:%v; want rollback error", active, err)
	}
	var queueStatus string
	var attempts int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$1`, queueJobID,
	).Scan(&queueStatus, &attempts); err != nil {
		t.Fatalf("read rolled-back cleanup Queue state: %v", err)
	}
	if queueStatus != queue.StatusLeased || attempts != 2 {
		t.Fatalf("rolled-back cleanup Queue state = %s/%d; want leased/2", queueStatus, attempts)
	}
	assertCleanupMarkersClaimed(t, admin, sessionID, cleanupID)

	if _, err := admin.ExecContext(context.Background(), `DROP TRIGGER fail_cleanup_marker_update ON session_runtime_status`); err != nil {
		t.Fatalf("drop cleanup rollback trigger: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `DROP FUNCTION fail_cleanup_marker_update()`); err != nil {
		t.Fatalf("drop cleanup rollback function: %v", err)
	}
	finalized, err := deliveryStore.FinalizeRuntimeCleanupExhaustion(context.Background(), capture.job, RuntimeDeliveryResult{})
	if err != nil || finalized.Status != RuntimeDeliveryRejected || !finalized.QueueLeaseSettled {
		t.Fatalf("retry cleanup exhaustion after rollback = %#v/%v", finalized, err)
	}
	assertCleanupMarkersRearmed(t, admin, sessionID, true)
}

func TestPostgreSQLCleanupTakeoverFencesBeforeSendAndAfterHostEffect(t *testing.T) {
	t.Run("takeover before final pre-send fence", func(t *testing.T) {
		runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const sessionID = "sesn_clean_presend"
		process := startCleanupComposition(t, "success", "pod_uid_"+sessionID)
		queueStore, deliveryStore, runner, queueJobID := seedCleanupComposition(
			t, runtimeDB, admin, process.port, sessionID, "thrd_clean_presend", "cleanup_presend", 3,
		)
		barrierStore := &cleanupAuthorityBarrierStore{
			PostgreSQLRuntimeDeliveryStore: deliveryStore,
			entered:                        make(chan struct{}), release: make(chan struct{}),
		}
		sender := &countingCleanupSender{RuntimeCommandSender: NewRuntimePodCommandClient(taskNotificationRuntimeTokenSource{})}
		runner.Deliverer = RuntimePodDirectDeliverer{Store: barrierStore, Sender: sender}
		done := make(chan error, 1)
		go func() { done <- runner.RunOnce(context.Background()) }()
		<-barrierStore.entered
		newLease := reclaimCleanupLease(t, admin, queueStore, queueJobID, "cleanup-presend-winner")
		close(barrierStore.release)
		if err := <-done; err != nil {
			t.Fatalf("old pre-send cleanup worker: %v", err)
		}
		if sender.calls.Load() != 0 {
			t.Fatalf("old pre-send worker made %d Runtime calls; want zero", sender.calls.Load())
		}
		assertExactCleanupLease(t, admin, queueJobID, newLease.LeaseToken)
	})

	t.Run("takeover after host effect before durable finalization", func(t *testing.T) {
		runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const sessionID = "sesn_clean_inflight"
		process := startCleanupComposition(t, "success", "pod_uid_"+sessionID)
		queueStore, deliveryStore, runner, queueJobID := seedCleanupComposition(
			t, runtimeDB, admin, process.port, sessionID, "thrd_clean_inflight", "cleanup_inflight", 3,
		)
		sender := &cleanupResponseBarrierSender{
			RuntimeCommandSender: NewRuntimePodCommandClient(taskNotificationRuntimeTokenSource{}),
			entered:              make(chan struct{}), release: make(chan struct{}), loseResponse: true,
		}
		runner.Deliverer = RuntimePodDirectDeliverer{Store: deliveryStore, Sender: sender}
		done := make(chan error, 1)
		go func() { done <- runner.RunOnce(context.Background()) }()
		<-sender.entered
		newLease := reclaimCleanupLease(t, admin, queueStore, queueJobID, "cleanup-inflight-winner")
		close(sender.release)
		if err := <-done; err != nil {
			t.Fatalf("old in-flight cleanup worker: %v", err)
		}
		if sender.lostResponses.Load() != 1 {
			t.Fatalf("old in-flight cleanup worker lost %d responses; want one", sender.lostResponses.Load())
		}
		assertExactCleanupLease(t, admin, queueJobID, newLease.LeaseToken)
		var bindings int
		if err := admin.QueryRowContext(context.Background(),
			`SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1`, sessionID,
		).Scan(&bindings); err != nil {
			t.Fatalf("read in-flight takeover binding: %v", err)
		}
		if bindings != 1 {
			t.Fatalf("old in-flight worker removed binding after lease loss")
		}

		winner := &JobRunner{
			Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
			Deliverer: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: NewRuntimePodCommandClient(taskNotificationRuntimeTokenSource{})},
			Config:    JobRunnerConfig{LeaseOwner: "cleanup-inflight-winner", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
		}
		if err := winner.processRuntimeJob(context.Background(), cleanupQueueJobProto(newLease), winner.Config); err != nil {
			t.Fatalf("winning cleanup worker: %v", err)
		}
		var effect struct {
			HostInvocations int `json:"hostInvocations"`
			HostEffects     int `json:"hostEffects"`
		}
		raw, err := os.ReadFile(process.effectPath)
		if err != nil || json.Unmarshal(raw, &effect) != nil || effect.HostInvocations != 2 || effect.HostEffects != 1 {
			t.Fatalf("cleanup host effect = %s/%v; want two convergent calls and one effect", raw, err)
		}
		var queueStatus string
		if err := admin.QueryRowContext(context.Background(),
			`SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$1`, queueJobID,
		).Scan(&queueStatus); err != nil || queueStatus != queue.StatusAcknowledged {
			t.Fatalf("winning cleanup Queue state = %s/%v; want acknowledged", queueStatus, err)
		}
	})
}

type cleanupAuthorityBarrierStore struct {
	*PostgreSQLRuntimeDeliveryStore
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *cleanupAuthorityBarrierStore) RuntimeCleanupDeliveryAuthority(ctx context.Context, job RuntimeJob) (RuntimeCleanupDeliveryAuthority, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return s.PostgreSQLRuntimeDeliveryStore.RuntimeCleanupDeliveryAuthority(ctx, job)
}

type countingCleanupSender struct {
	RuntimeCommandSender
	calls atomic.Int32
}

func (s *countingCleanupSender) CleanupSession(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.CleanupSessionRequest) (*agentruntimev1.CleanupSessionResponse, error) {
	s.calls.Add(1)
	return s.RuntimeCommandSender.CleanupSession(ctx, target, request)
}

type cleanupResponseBarrierSender struct {
	RuntimeCommandSender
	entered       chan struct{}
	release       chan struct{}
	loseResponse  bool
	lostResponses atomic.Int32
	once          sync.Once
}

type cleanupLostResponseSender struct {
	RuntimeCommandSender
	lost atomic.Bool
}

type cleanupInvalidResponseSender struct {
	RuntimeCommandSender
	calls atomic.Int32
}

func (s *cleanupInvalidResponseSender) CleanupSession(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.CleanupSessionRequest) (*agentruntimev1.CleanupSessionResponse, error) {
	response, err := s.RuntimeCommandSender.CleanupSession(ctx, target, request)
	if err != nil {
		return response, err
	}
	s.calls.Add(1)
	return &agentruntimev1.CleanupSessionResponse{}, nil
}

func (s *cleanupLostResponseSender) CleanupSession(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.CleanupSessionRequest) (*agentruntimev1.CleanupSessionResponse, error) {
	response, err := s.RuntimeCommandSender.CleanupSession(ctx, target, request)
	if err == nil && !s.lost.Swap(true) {
		return nil, context.DeadlineExceeded
	}
	return response, err
}

func (s *cleanupResponseBarrierSender) CleanupSession(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.CleanupSessionRequest) (*agentruntimev1.CleanupSessionResponse, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	response, err := s.RuntimeCommandSender.CleanupSession(ctx, target, request)
	if err == nil && s.loseResponse {
		s.lostResponses.Add(1)
		return nil, context.DeadlineExceeded
	}
	return response, err
}

type cleanupExhaustionResponseLossDeliverer struct {
	RuntimePodDirectDeliverer
	job  RuntimeJob
	lost bool
}

func (d *cleanupExhaustionResponseLossDeliverer) FinalizeRuntimeCleanupExhaustion(ctx context.Context, job RuntimeJob, result RuntimeDeliveryResult) (RuntimeDeliveryResult, error) {
	d.job = job
	finalized, err := d.RuntimePodDirectDeliverer.FinalizeRuntimeCleanupExhaustion(ctx, job, result)
	if err == nil && finalized.QueueLeaseSettled && !d.lost {
		d.lost = true
		return RuntimeDeliveryResult{}, context.DeadlineExceeded
	}
	return finalized, err
}

func startCleanupComposition(t *testing.T, mode string, podUID string) *cleanupCompositionProcess {
	t.Helper()
	tempDir := t.TempDir()
	readyPath := filepath.Join(tempDir, "ready.json")
	process := &cleanupCompositionProcess{
		effectPath: filepath.Join(tempDir, "effect.json"),
		closePath:  filepath.Join(tempDir, "close"),
	}
	input, err := json.Marshal(map[string]any{
		"targetPodUid": podUID, "sessionId": strings.TrimPrefix(podUID, "pod_uid_"), "mode": mode, "readyPath": readyPath,
		"effectPath": process.effectPath, "closePath": process.closePath,
	})
	if err != nil {
		t.Fatalf("encode cleanup composition: %v", err)
	}
	inputPath := filepath.Join(tempDir, "input.json")
	if err := os.WriteFile(inputPath, input, 0o600); err != nil {
		t.Fatalf("write cleanup composition input: %v", err)
	}
	process.command = exec.Command("bun", "packages/runtime-pod/test/fixtures/cleanup-production-composition.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	process.command.Dir = "../agent-runtime"
	process.command.Stdout = &process.output
	process.command.Stderr = &process.output
	if err := process.command.Start(); err != nil {
		t.Fatalf("start cleanup composition: %v", err)
	}
	t.Cleanup(func() {
		_ = os.WriteFile(process.closePath, []byte("close"), 0o600)
		if process.command.ProcessState == nil {
			_ = process.command.Wait()
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		raw, readErr := os.ReadFile(readyPath)
		var ready struct {
			Port int `json:"port"`
		}
		if readErr == nil && json.Unmarshal(raw, &ready) == nil && ready.Port > 0 {
			process.port = ready.Port
			return process
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("cleanup composition did not become ready: %s", process.output.String())
	return nil
}

func seedCleanupComposition(t *testing.T, runtimeDB, admin *sql.DB, port int, sessionID, threadID, cleanupID string, maxAttempts int) (*queue.PostgreSQLQueueStore, *PostgreSQLRuntimeDeliveryStore, *JobRunner, string) {
	t.Helper()
	seedBridgeCleanupTreeFixture(t, runtimeDB, admin, sessionID, threadID, "", cleanupID, false)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_bindings
		    SET agent_runtime_pod_name='runtime-cleanup-composition', agent_runtime_pod_ip='127.0.0.1'
		  WHERE workspace_id='default' AND session_id=$1`, sessionID,
	); err != nil {
		t.Fatalf("route cleanup binding to composition Runtime: %v", err)
	}
	client := dbconnect.NewClientForTesting(runtimeDB)
	queueStore := queue.NewPostgreSQLStore(client)
	queueJobID := "qjob_" + cleanupID
	if _, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
		ID: queueJobID, WorkspaceID: workspace.DefaultID, Kind: queue.KindCleanupSession,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:      queue.FormatCleanupSessionDedupeKey(workspace.DefaultID, sessionID, cleanupID),
		PayloadVersion: 1,
		PayloadJSON:    []byte(`{"workspace_id":"default","session_id":"` + sessionID + `","cleanup_job_id":"` + cleanupID + `"}`),
		MaxAttempts:    maxAttempts,
	}); err != nil {
		t.Fatalf("enqueue cleanup composition: %v", err)
	}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(client, port)
	deliveryStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC) }
	deliveryStore.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "tetral-agent-runtime", PodName: "runtime-cleanup-composition", PodUID: "pod_uid_" + sessionID, PodIP: net.IPv4(127, 0, 0, 1).String(),
		}})
	}}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: NewRuntimePodCommandClient(taskNotificationRuntimeTokenSource{})},
		Config:    JobRunnerConfig{LeaseOwner: "cleanup-composition", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	return queueStore, deliveryStore, runner, queueJobID
}

func assertCleanupMarkersRearmed(t *testing.T, admin *sql.DB, sessionID string, requireAfter bool) time.Time {
	t.Helper()
	var cleanupAfter sql.NullTime
	var cleanupJobID, cleanupEnqueuedAt, cleanupClaimedAt sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT cleanup_after, cleanup_job_id, cleanup_enqueued_at, cleanup_claimed_at
		   FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1`, sessionID,
	).Scan(&cleanupAfter, &cleanupJobID, &cleanupEnqueuedAt, &cleanupClaimedAt); err != nil {
		t.Fatalf("read cleanup markers: %v", err)
	}
	if requireAfter && !cleanupAfter.Valid {
		t.Fatal("cleanup_after was not rearmed")
	}
	if cleanupJobID.Valid || cleanupEnqueuedAt.Valid || cleanupClaimedAt.Valid {
		t.Fatalf("cleanup markers were not released: %v/%v/%v", cleanupJobID, cleanupEnqueuedAt, cleanupClaimedAt)
	}
	return cleanupAfter.Time
}

func assertCleanupMarkersClaimed(t *testing.T, admin *sql.DB, sessionID, cleanupID string) {
	t.Helper()
	var cleanupJobID sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT cleanup_job_id FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1`, sessionID,
	).Scan(&cleanupJobID); err != nil {
		t.Fatalf("read claimed cleanup markers: %v", err)
	}
	if !cleanupJobID.Valid || cleanupJobID.String != cleanupID {
		t.Fatalf("claimed cleanup job = %v; want %s", cleanupJobID, cleanupID)
	}
}

func reclaimCleanupLease(t *testing.T, admin *sql.DB, store *queue.PostgreSQLQueueStore, queueJobID, owner string) *queue.Job {
	t.Helper()
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second' WHERE workspace_id='default' AND id=$1`, queueJobID,
	); err != nil {
		t.Fatalf("expire cleanup lease: %v", err)
	}
	if reclaimed, err := store.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{
		WorkspaceID: workspace.DefaultID, Kind: queue.KindCleanupSession, Limit: 1,
	}); err != nil || reclaimed != 1 {
		t.Fatalf("reclaim cleanup lease = %d/%v; want 1/nil", reclaimed, err)
	}
	leased, err := store.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindCleanupSession},
		LeaseOwner: owner, MaxJobs: 1, LeaseDuration: time.Minute,
	})
	if err != nil || len(leased) != 1 || leased[0].ID != queueJobID {
		t.Fatalf("lease reclaimed cleanup = %#v/%v", leased, err)
	}
	return leased[0]
}

func assertExactCleanupLease(t *testing.T, admin *sql.DB, queueJobID, leaseToken string) {
	t.Helper()
	var status, currentToken string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, lease_token FROM queue_jobs WHERE workspace_id='default' AND id=$1`, queueJobID,
	).Scan(&status, &currentToken); err != nil {
		t.Fatalf("read exact cleanup lease: %v", err)
	}
	if status != queue.StatusLeased || currentToken != leaseToken {
		t.Fatalf("cleanup lease = %s/%s; want leased/%s", status, currentToken, leaseToken)
	}
}

func cleanupQueueJobProto(job *queue.Job) *queuev1.QueueJob {
	return &queuev1.QueueJob{
		Id: job.ID, WorkspaceId: string(job.WorkspaceID), Kind: job.Kind,
		PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
		PayloadJson: string(job.PayloadJSON), LeaseToken: job.LeaseToken,
		AttemptCount: int32(job.AttemptCount), MaxAttempts: int32(job.MaxAttempts),
	}
}
