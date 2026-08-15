package agentruntimebridge

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestPostgreSQLAcceptanceTimeTaskNotificationParkingLogsCommittedCustody(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_accept_park_log"
		parentID  = "thr_accept_park_log_parent"
		childID   = "thr_accept_park_log_child"
		bindingID = "bind_accept_park_log"
		podUID    = "pod_accept_park_log"
		taskID    = "task_accept_park_log"
		inputID   = "task_notification:task_accept_park_log"
	)
	now := time.Date(2026, 8, 11, 1, 0, 0, 0, time.UTC)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIBackgroundTask(t, admin, "default", sessionID, childID, bindingID, taskID, "evt_accept_park_log_task")
	settleBridgeAPIBackgroundTask(t, admin, sessionID, taskID, "completed", `{"status":"completed","stdout":{"text":"canary-secret-payload","truncated":false},"stderr":{"text":"","truncated":false}}`)
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_inbox (
		workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,event_ids_json,status,created_at,updated_at
	) VALUES ('default',$1,$2,$3,'task_notification','[]','queued',$4,$4)`, sessionID, childID, inputID, now); err != nil {
		t.Fatalf("seed task notification Inbox custody: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueue, err := queue.NewTaskNotificationRuntimeInputEnqueueRequest(workspace.DefaultID, sessionID, childID, taskID, now)
	if err != nil {
		t.Fatalf("build task notification Queue custody: %v", err)
	}
	if _, err := queueStore.Enqueue(context.Background(), enqueue); err != nil {
		t.Fatalf("enqueue task notification Queue custody: %v", err)
	}
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "bridge-accept-park-log",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(time.Second),
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease task notification = %#v, %v; want one", leased, err)
	}
	job, err := DecodeRuntimeJob(queueJobProto(leased[0]))
	if err != nil {
		t.Fatalf("decode task notification: %v", err)
	}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	deliveryStore.Clock = func() time.Time { return now.Add(2 * time.Second) }
	deliveryStore.TargetResolver = &recordingRuntimeTargetResolver{binding: runtimeBindingForDelivery{
		BindingID: bindingID, BindingGeneration: 1, Namespace: "runtime-ns", PodName: "runtime-pod",
		PodUID: podUID, PodIP: "10.0.0.1",
	}}
	plan, err := deliveryStore.PrepareRuntimeCommand(context.Background(), job)
	if err != nil || plan.AcceptInput == nil {
		t.Fatalf("prepare task notification = %#v, %v; want Runtime request", plan, err)
	}

	source := seedBridgeAPIChildLifecycleToolSource(t, admin, sessionID, parentID, "evt_accept_park_log_close")
	apiStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	if _, err := apiStore.AdmitChildInterrupt(context.Background(), &bridgev1.AdmitChildInterruptRequest{
		Scope: bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID), SourceToolUseEventId: source,
	}); err != nil {
		t.Fatalf("admit close fence: %v", err)
	}
	var output bytes.Buffer
	deliveryStore.Logger = slog.New(slog.NewJSONHandler(&output, nil))
	settled, err := deliveryStore.MarkRuntimeInputAccepted(context.Background(), job, plan.AttemptedBinding)
	if err != nil || !settled {
		t.Fatalf("accept notification behind close fence = %t, %v; want lease settlement", settled, err)
	}
	var inboxStatus, queueStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT inbox.status,job.status
		FROM session_runtime_inbox inbox JOIN queue_jobs job ON job.workspace_id=inbox.workspace_id AND job.id=$2
		WHERE inbox.workspace_id='default' AND inbox.runtime_input_id=$1`, inputID, leased[0].ID).Scan(&inboxStatus, &queueStatus); err != nil {
		t.Fatalf("read parked custody: %v", err)
	}
	if inboxStatus != "parked" || queueStatus != queue.StatusAcknowledged {
		t.Fatalf("parked custody = Inbox %q / Queue %q; want parked / acknowledged", inboxStatus, queueStatus)
	}
	text := output.String()
	for _, fragment := range []string{
		`"event.kind":"runtime_input_custody_transition"`, `"workspace.id":"default"`,
		`"session.id":"` + sessionID + `"`, `"thread.id":"` + childID + `"`,
		`"outcome":"accepted_to_parked"`, `"input.count":1`,
	} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("acceptance-time custody log missing %s: %s", fragment, text)
		}
	}
	for _, forbidden := range []string{"canary-secret-payload", "tool_input", "terminal_result_json", "credential"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("acceptance-time custody log contains forbidden field %q: %s", forbidden, text)
		}
	}
}

func TestPostgreSQLPrepareTaskNotificationParksQueuedCustodyBeforeRuntimeResolution(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_prepare_park"
		parentID  = "thr_prepare_park_parent"
		childID   = "thr_prepare_park_child"
		bindingID = "bind_prepare_park"
		podUID    = "pod_prepare_park"
		taskID    = "task_prepare_park"
		inputID   = "task_notification:task_prepare_park"
	)
	now := time.Date(2026, 8, 11, 2, 0, 0, 0, time.UTC)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIBackgroundTask(t, admin, "default", sessionID, childID, bindingID, taskID, "evt_prepare_park_task")
	settleBridgeAPIBackgroundTask(t, admin, sessionID, taskID, "completed", `{"status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`)
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_inbox (
		workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,event_ids_json,status,created_at,updated_at
	) VALUES ('default',$1,$2,$3,'task_notification','[]','queued',$4,$4)`, sessionID, childID, inputID, now); err != nil {
		t.Fatalf("seed task notification Inbox custody: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueue, err := queue.NewTaskNotificationRuntimeInputEnqueueRequest(workspace.DefaultID, sessionID, childID, taskID, now)
	if err != nil {
		t.Fatalf("build task notification Queue custody: %v", err)
	}
	if _, err := queueStore.Enqueue(context.Background(), enqueue); err != nil {
		t.Fatalf("enqueue task notification Queue custody: %v", err)
	}
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "bridge-prepare-park",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(time.Second),
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease task notification = %#v, %v; want one", leased, err)
	}
	job, err := DecodeRuntimeJob(queueJobProto(leased[0]))
	if err != nil {
		t.Fatalf("decode task notification: %v", err)
	}
	source := seedBridgeAPIChildLifecycleToolSource(t, admin, sessionID, parentID, "evt_prepare_park_close")
	apiStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	if _, err := apiStore.AdmitChildInterrupt(context.Background(), &bridgev1.AdmitChildInterruptRequest{
		Scope: bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID), SourceToolUseEventId: source,
	}); err != nil {
		t.Fatalf("admit close fence: %v", err)
	}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	resolver := &recordingRuntimeTargetResolver{err: errors.New("target resolution must not run for parked custody")}
	deliveryStore.TargetResolver = resolver
	plan, err := deliveryStore.PrepareRuntimeCommand(context.Background(), job)
	if err != nil || !plan.SettledAccepted || !plan.QueueLeaseSettled || plan.hasCommand() {
		t.Fatalf("prepare notification behind close fence = %#v, %v; want settled parked custody", plan, err)
	}
	if len(resolver.jobs) != 0 {
		t.Fatalf("target resolver calls = %d; want zero", len(resolver.jobs))
	}
	var inboxStatus, queueStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT inbox.status,job.status
		FROM session_runtime_inbox inbox JOIN queue_jobs job ON job.workspace_id=inbox.workspace_id AND job.id=$2
		WHERE inbox.workspace_id='default' AND inbox.runtime_input_id=$1`, inputID, leased[0].ID).Scan(&inboxStatus, &queueStatus); err != nil {
		t.Fatalf("read prepare-time parked custody: %v", err)
	}
	if inboxStatus != "parked" || queueStatus != queue.StatusAcknowledged {
		t.Fatalf("prepare-time custody = Inbox %q / Queue %q; want parked / acknowledged", inboxStatus, queueStatus)
	}
}

func TestPostgreSQLChildCloseAdmissionParksQueuedTaskNotificationAndCancelsQueue(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_close_park_queued"
		parentID  = "thr_close_park_parent"
		childID   = "thr_close_park_child"
		bindingID = "bind_close_park"
		podUID    = "pod_close_park"
		taskID    = "task_close_park"
		inputID   = "task_notification:task_close_park"
	)
	now := time.Date(2026, 8, 11, 4, 0, 0, 0, time.UTC)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_inbox (
		workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,event_ids_json,status,created_at,updated_at
	) VALUES ('default',$1,$2,$3,'task_notification','[]','queued',$4,$4)`, sessionID, childID, inputID, now); err != nil {
		t.Fatalf("seed queued task notification: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueue, err := queue.NewTaskNotificationRuntimeInputEnqueueRequest(workspace.DefaultID, sessionID, childID, taskID, now)
	if err != nil {
		t.Fatalf("build queued task notification: %v", err)
	}
	if _, err := queueStore.Enqueue(context.Background(), enqueue); err != nil {
		t.Fatalf("enqueue queued task notification: %v", err)
	}
	source := seedBridgeAPIChildLifecycleToolSource(t, admin, sessionID, parentID, "evt_close_park_queued")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	request := &bridgev1.AdmitChildInterruptRequest{
		Scope: bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID), SourceToolUseEventId: source,
	}
	first, err := store.AdmitChildInterrupt(context.Background(), request)
	if err != nil || first.GetCommitted() == nil {
		t.Fatalf("admit close over queued notification = %#v, %v; want committed", first, err)
	}
	replay, err := store.AdmitChildInterrupt(context.Background(), request)
	if err != nil || replay.GetDuplicate() == nil || replay.GetDuplicate().GetControlOperationId() != first.GetCommitted().GetControlOperationId() {
		t.Fatalf("replay close admission = %#v, %v; want duplicate", replay, err)
	}
	var inboxStatus, queueStatus, parkedBinding, parkedPod string
	var parkedGeneration int64
	if err := admin.QueryRowContext(context.Background(), `SELECT inbox.status,job.status,
		inbox.binding_id,inbox.binding_generation,inbox.target_pod_uid
		FROM session_runtime_inbox inbox JOIN queue_jobs job
		 ON job.workspace_id=inbox.workspace_id AND job.dedupe_key='runtime_input:' || inbox.workspace_id || ':' || inbox.session_id || ':' || inbox.runtime_input_id
		WHERE inbox.workspace_id='default' AND inbox.runtime_input_id=$1`, inputID).Scan(
		&inboxStatus, &queueStatus, &parkedBinding, &parkedGeneration, &parkedPod,
	); err != nil {
		t.Fatalf("read close-admission custody: %v", err)
	}
	if inboxStatus != "parked" || queueStatus != queue.StatusCancelled ||
		parkedBinding != bindingID || parkedGeneration != 1 || parkedPod != podUID {
		t.Fatalf("close-admission custody = Inbox %q Queue %q binding %q/%d/%q; want parked/cancelled exact binding", inboxStatus, queueStatus, parkedBinding, parkedGeneration, parkedPod)
	}
}
