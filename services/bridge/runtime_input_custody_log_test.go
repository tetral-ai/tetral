package agentruntimebridge

import (
	"context"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

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
		TargetChildThreadId: childID, Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE,
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
