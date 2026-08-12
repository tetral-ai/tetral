package agentruntimebridge

import (
	"context"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
)

func TestPostgreSQLJobRunnerDeliversProducerQueuedMessageInput(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_first_queued_delivery"
		threadID  = "thr_first_queued_delivery"
		bindingID = "bind_first_queued_delivery"
		podUID    = "pod_first_queued_delivery"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)

	client := dbconnect.NewClientForTesting(runtime)
	eventService := sessionevent.NewService(sessionevent.NewPostgreSQLStore(client))
	appended, err := eventService.AppendClientEvents(
		context.Background(),
		workspace.DefaultID,
		sessionID,
		"idem_first_queued_delivery",
		sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{
			Type: sessionevent.EventTypeUserMessage,
			Content: []sessionevent.ContentBlock{{
				Type: sessionevent.ContentBlockTypeText,
				Text: "start the first turn",
			}},
		}}},
	)
	if err != nil || len(appended.Data) != 1 {
		t.Fatalf("append first user message = %#v, %v; want one durable event", appended, err)
	}

	var runtimeInputID string
	var initialInboxStatus string
	var initialQueueStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT runtime_input_id, status
		FROM session_runtime_inbox
		WHERE workspace_id='default' AND session_id=$1 AND input_kind='messages'`, sessionID).
		Scan(&runtimeInputID, &initialInboxStatus); err != nil {
		t.Fatalf("read producer-created Runtime Inbox: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status
		FROM queue_jobs
		WHERE workspace_id='default' AND payload_json::jsonb ->> 'runtime_input_id'=$1`, runtimeInputID).
		Scan(&initialQueueStatus); err != nil {
		t.Fatalf("read producer-created Queue job: %v", err)
	}
	if initialInboxStatus != "queued" || initialQueueStatus != queue.StatusPending {
		t.Fatalf("producer custody = Inbox %q / Queue %q; want queued / pending", initialInboxStatus, initialQueueStatus)
	}

	candidate := enginekubernetes.BindingCandidate{
		Namespace: "tetral-agent-runtime",
		PodName:   "runtime-pod-0",
		PodUID:    podUID,
		PodIP:     "10.0.0.10",
	}
	deliveryStore := NewJobRunnerRuntimeDeliveryStore(
		client,
		nil,
		JobRunnerConfig{AgentRuntimeGRPCPort: 9090},
		func() enginekubernetes.BindingVisibilitySnapshot {
			return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{candidate})
		},
	)
	sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{
		Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED,
	}}
	runner := &JobRunner{
		Queue:      tetralqueue.NewServer(queue.NewPostgreSQLStore(client), nil),
		Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer:  RuntimePodDirectDeliverer{Store: deliveryStore, Sender: sender},
		Config: JobRunnerConfig{
			LeaseOwner:        "bridge-first-queued-delivery",
			LeaseDuration:     time.Minute,
			HeartbeatInterval: 20 * time.Second,
			MaxJobs:           1,
		},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("run first queued delivery: %v", err)
	}
	if len(sender.requests) != 1 || sender.requests[0].GetRuntimeInputId() != runtimeInputID {
		t.Fatalf("Runtime requests = %#v; want one request for %s", sender.requests, runtimeInputID)
	}

	var finalInboxStatus string
	var finalQueueStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT status
		FROM session_runtime_inbox
		WHERE workspace_id='default' AND runtime_input_id=$1`, runtimeInputID).
		Scan(&finalInboxStatus); err != nil {
		t.Fatalf("read delivered Runtime Inbox: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status
		FROM queue_jobs
		WHERE workspace_id='default' AND payload_json::jsonb ->> 'runtime_input_id'=$1`, runtimeInputID).
		Scan(&finalQueueStatus); err != nil {
		t.Fatalf("read settled Queue job: %v", err)
	}
	if finalInboxStatus != "accepted" || finalQueueStatus != queue.StatusAcknowledged {
		t.Fatalf("delivered custody = Inbox %q / Queue %q; want accepted / acknowledged", finalInboxStatus, finalQueueStatus)
	}
}
