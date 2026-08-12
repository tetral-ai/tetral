package agentruntimebridge

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestPostgreSQLTaskNotificationLeaseAndChildCloseSerializeWithoutStrandedCustody(t *testing.T) {
	for _, admissionFirst := range []bool{true, false} {
		name := "close_admission_holds_session_lock_first"
		if !admissionFirst {
			name = "queue_runner_enters_prepare_first"
		}
		t.Run(name, func(t *testing.T) {
			runTaskNotificationCloseLeaseRace(t, admissionFirst)
		})
	}
}

func runTaskNotificationCloseLeaseRace(t *testing.T, admissionFirst bool) {
	t.Helper()
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_task_close_lease_race"
		parentID  = "thr_task_close_lease_parent"
		childID   = "thr_task_close_lease_child"
		bindingID = "bind_task_close_lease"
		podUID    = "pod_task_close_lease"
		taskID    = "task_close_lease"
		inputID   = "task_notification:task_close_lease"
	)
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPINotifiableBackgroundTask(t, admin, "default", sessionID, childID, bindingID, taskID, "evt_task_close_lease_source")
	settleBridgeAPIBackgroundTask(t, admin, sessionID, taskID, "completed", `{"task_id":"task_close_lease","source_tool_use_event_id":"evt_task_close_lease_source","status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`)
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_inbox (
		workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,event_ids_json,status,created_at,updated_at
	) VALUES ('default',$1,$2,$3,'task_notification','[]','queued',$4,$4)`, sessionID, childID, inputID, now); err != nil {
		t.Fatalf("seed task notification Inbox: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueue, err := queue.NewTaskNotificationRuntimeInputEnqueueRequest(workspace.DefaultID, sessionID, childID, taskID, now)
	if err != nil {
		t.Fatalf("build task notification Queue custody: %v", err)
	}
	queued, err := queueStore.Enqueue(context.Background(), enqueue)
	if err != nil {
		t.Fatalf("enqueue task notification Queue custody: %v", err)
	}
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "bridge-task-close-race",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(time.Second),
	})
	if err != nil || len(leased) != 1 || leased[0].ID != queued.ID {
		t.Fatalf("lease task notification = %#v, %v; want %s", leased, err, queued.ID)
	}
	job, err := DecodeRuntimeJob(queueJobProto(leased[0]))
	if err != nil {
		t.Fatalf("decode task notification: %v", err)
	}

	closeSource := seedBridgeAPIChildLifecycleToolSource(t, admin, sessionID, parentID, "evt_task_close_lease_close")
	parentScope := bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID)
	apiStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	apiStore.Clock = func() time.Time { return now.Add(2 * time.Second) }
	admitRequest := &bridgev1.AdmitChildInterruptRequest{
		Scope: parentScope, RootChildThreadId: childID,
		SourceToolUseEventId: closeSource.GetSourceToolUseEventId(), Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE,
		IncludeDescendants: true,
	}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	deliveryStore.Clock = func() time.Time { return now.Add(2 * time.Second) }

	blocker, err := admin.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin Session lock blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback() }()
	var lockedSession string
	if err := blocker.QueryRow(`SELECT id FROM sessions WHERE workspace_id='default' AND id=$1 FOR UPDATE`, sessionID).Scan(&lockedSession); err != nil {
		t.Fatalf("hold Session lock: %v", err)
	}
	var blockerPID int
	if err := blocker.QueryRow(`SELECT pg_backend_pid()`).Scan(&blockerPID); err != nil {
		t.Fatalf("read blocker pid: %v", err)
	}
	type raceResult struct {
		kind  string
		admit *bridgev1.AdmitChildInterruptResponse
		plan  RuntimeCommandPlan
		err   error
	}
	results := make(chan raceResult, 2)
	startAdmission := func() {
		go func() {
			response, err := apiStore.AdmitChildInterrupt(context.Background(), admitRequest)
			results <- raceResult{kind: "admit", admit: response, err: err}
		}()
	}
	startPrepare := func() {
		go func() {
			plan, err := deliveryStore.PrepareRuntimeCommand(context.Background(), job)
			results <- raceResult{kind: "prepare", plan: plan, err: err}
		}()
	}
	if admissionFirst {
		startAdmission()
		waitForPostgreSQLLockWaiters(t, admin, blockerPID, 1)
		startPrepare()
	} else {
		startPrepare()
		waitForPostgreSQLLockWaiters(t, admin, blockerPID, 1)
		startAdmission()
	}
	waitForPostgreSQLLockWaiters(t, admin, blockerPID, 2)
	if err := blocker.Commit(); err != nil {
		t.Fatalf("release Session lock: %v", err)
	}
	var admitted *bridgev1.AdmitChildInterruptResponse
	var initialPlan RuntimeCommandPlan
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("%s race operation: %v", result.kind, result.err)
		}
		if result.kind == "admit" {
			admitted = result.admit
		} else {
			initialPlan = result.plan
		}
	}
	if admitted == nil || len(admitted.GetTargets()) != 1 {
		t.Fatalf("close admission = %#v; want one frozen target", admitted)
	}
	if admissionFirst {
		if !initialPlan.SettledAccepted || !initialPlan.QueueLeaseSettled || initialPlan.Request != nil {
			t.Fatalf("admission-first prepare = %#v; want parked Inbox and lease-fenced Queue ACK", initialPlan)
		}
	} else if initialPlan.Request == nil || initialPlan.SettledAccepted {
		t.Fatalf("runner-first prepare = %#v; want live Runtime request before close fence", initialPlan)
	}

	target := admitted.GetTargets()[0]
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_inbox
		SET status='accepted',binding_id=$2,binding_generation=1,target_pod_uid=$3
		WHERE workspace_id='default' AND runtime_input_id=$1`, target.GetRuntimeInputId(), bindingID, podUID); err != nil {
		t.Fatalf("accept child-close control: %v", err)
	}
	childScope := scopeForThread(parentScope, childID)
	if _, err := apiStore.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope: childScope, RuntimeInputId: target.GetRuntimeInputId(), InputKind: "interrupt_control",
		EventIds: []string{target.GetInterruptEventId()}, SequenceFrom: target.GetInterruptEventSequence(), SequenceTo: target.GetInterruptEventSequence(),
	}); err != nil {
		t.Fatalf("commit child-close control: %v", err)
	}
	awaitRequest := &bridgev1.AwaitChildInterruptRequest{
		Scope: parentScope, RootChildThreadId: childID, SourceToolUseEventId: closeSource.GetSourceToolUseEventId(),
		Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE, IncludeDescendants: true, Targets: admitted.GetTargets(),
	}
	closeSourceID := closeSource.GetSourceToolUseEventId()
	closeRequest := &bridgev1.MarkChildThreadClosedRequest{
		Scope: parentScope, ChildThreadId: childID, Source: closeSource,
		SourceToolUseEventId: &closeSourceID, Targets: admitted.GetTargets(),
	}
	controlQueueSettled := false
	if !admissionFirst {
		takeoverNow := now.Add(7 * 24 * time.Hour)
		if response, err := apiStore.AwaitChildInterrupt(context.Background(), awaitRequest); status.Code(err) != codes.DeadlineExceeded || response != nil {
			t.Fatalf("await before leased notification quiescence = %#v/%v; want DeadlineExceeded", response, err)
		}
		if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second'
			WHERE workspace_id='default' AND id=$1 AND status='leased'`, queued.ID); err != nil {
			t.Fatalf("expire original task notification lease: %v", err)
		}
		if reclaimed, err := queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{
			WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput,
		}); err != nil || reclaimed != 1 {
			t.Fatalf("reclaim expired task notification lease = %d, %v; want one", reclaimed, err)
		}
		takeover, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
			WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "bridge-task-close-takeover",
			MaxJobs: 1, LeaseDuration: time.Minute, Now: takeoverNow,
		})
		if err != nil || len(takeover) != 1 {
			t.Fatalf("take over close-race Queue work = %#v, %v; want one", takeover, err)
		}
		if takeover[0].ID != queued.ID {
			if acked, err := queueStore.Ack(context.Background(), queue.AckRequest{
				WorkspaceID: workspace.DefaultID, JobID: takeover[0].ID, LeaseToken: takeover[0].LeaseToken, Now: takeoverNow.Add(time.Second),
			}); err != nil || !acked {
				t.Fatalf("ACK overtaking committed child-close control = %t, %v; want true", acked, err)
			}
			controlQueueSettled = true
			takeover, err = queueStore.Lease(context.Background(), queue.LeaseRequest{
				WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "bridge-task-close-takeover",
				MaxJobs: 1, LeaseDuration: time.Minute, Now: takeoverNow.Add(2 * time.Second),
			})
			if err != nil || len(takeover) != 1 || takeover[0].ID != queued.ID {
				t.Fatalf("take over expired task notification after control = %#v, %v; want %s", takeover, err, queued.ID)
			}
		}
		takeoverJob, err := DecodeRuntimeJob(queueJobProto(takeover[0]))
		if err != nil {
			t.Fatalf("decode takeover job: %v", err)
		}
		takeoverPlan, err := deliveryStore.PrepareRuntimeCommand(context.Background(), takeoverJob)
		if err != nil || !takeoverPlan.SettledAccepted || !takeoverPlan.QueueLeaseSettled {
			t.Fatalf("takeover prepare = %#v/%v; want parked Inbox and Queue ACK", takeoverPlan, err)
		}
	}
	if _, err := apiStore.AwaitChildInterrupt(context.Background(), awaitRequest); err != nil {
		t.Fatalf("await child-close control after notification quiescence: %v", err)
	}

	if !controlQueueSettled {
		controlLease, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
			WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "bridge-child-close-control",
			MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(7*24*time.Hour + 3*time.Second),
		})
		if err != nil || len(controlLease) != 1 || controlLease[0].ID == queued.ID {
			t.Fatalf("lease committed child-close control = %#v, %v; want control job", controlLease, err)
		}
		if acked, err := queueStore.Ack(context.Background(), queue.AckRequest{
			WorkspaceID: workspace.DefaultID, JobID: controlLease[0].ID, LeaseToken: controlLease[0].LeaseToken, Now: now.Add(7*24*time.Hour + 4*time.Second),
		}); err != nil || !acked {
			t.Fatalf("ACK committed child-close control = %t, %v; want true", acked, err)
		}
	}
	closed, err := apiStore.MarkChildThreadClosed(context.Background(), closeRequest)
	if err != nil || closed.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("close after notification quiescence = %#v/%v; want committed receipt", closed, err)
	}
	var inboxStatus, taskQueueStatus, childStatus string
	var activeJobs, errorEvents int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$3 AND id=$4),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND partition_key='session:default:' || $3 AND status IN ('pending','leased')),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND type='session.error')`,
		inputID, queued.ID, sessionID, childID,
	).Scan(&inboxStatus, &taskQueueStatus, &childStatus, &activeJobs, &errorEvents); err != nil {
		t.Fatalf("read final close custody: %v", err)
	}
	if inboxStatus != "parked" || taskQueueStatus != queue.StatusAcknowledged || childStatus != "closed_for_runtime" || activeJobs != 0 || errorEvents != 0 {
		t.Fatalf("final close custody = Inbox %s Queue %s child %s active %d errors %d; want parked/acknowledged/closed/0/0",
			inboxStatus, taskQueueStatus, childStatus, activeJobs, errorEvents)
	}
}
