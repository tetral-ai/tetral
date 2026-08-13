package agentruntimebridge

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"reflect"
	"slices"
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

func TestPostgreSQLJobRunnerTerminalizesProducerQueuedMessageBeforeFirstClaim(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_first_queued_exhaustion"
		threadID  = "thr_first_queued_exhaustion"
		bindingID = "bind_first_queued_exhaustion"
		podUID    = "pod_first_queued_exhaustion"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	client := dbconnect.NewClientForTesting(runtime)
	eventService := sessionevent.NewService(sessionevent.NewPostgreSQLStore(client))
	appended, err := eventService.AppendClientEvents(
		context.Background(), workspace.DefaultID, sessionID, "idem_first_queued_exhaustion",
		sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{
			Type:    sessionevent.EventTypeUserMessage,
			Content: []sessionevent.ContentBlock{{Type: sessionevent.ContentBlockTypeText, Text: "exhaust before claim"}},
		}}},
	)
	if err != nil || len(appended.Data) != 1 {
		t.Fatalf("append producer input = %#v/%v; want one event", appended, err)
	}
	if _, err := admin.ExecContext(context.Background(), `DELETE FROM session_runtime_bindings
		WHERE workspace_id='default' AND session_id=$1`, sessionID); err != nil {
		t.Fatalf("remove Runtime target before first claim: %v", err)
	}
	var runtimeInputID string
	if err := admin.QueryRowContext(context.Background(), `SELECT runtime_input_id FROM session_runtime_inbox
		WHERE workspace_id='default' AND session_id=$1 AND input_kind='messages'`, sessionID).Scan(&runtimeInputID); err != nil {
		t.Fatalf("read producer Runtime input: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs
		SET max_attempts=1, available_at=clock_timestamp()-interval '1 second'
		WHERE workspace_id='default' AND kind='runtime_input'
		AND payload_json::jsonb ->> 'runtime_input_id'=$1`, runtimeInputID); err != nil {
		t.Fatalf("configure final producer input attempt: %v", err)
	}
	sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED}}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(client, 9090)
	var attemptLog bytes.Buffer
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queue.NewPostgreSQLStore(client), nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: manifestCompositionDeliverer{direct: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: sender}},
		Config:    JobRunnerConfig{MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
		Logger:    slog.New(slog.NewJSONHandler(&attemptLog, nil)),
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("run producer input final attempt: %v", err)
	}
	var inboxStatus, queueStatus, errorKind string
	var sessionErrors, liveInbox int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND payload_json::jsonb ->> 'runtime_input_id'=$1),
		(SELECT last_error_kind FROM queue_jobs WHERE workspace_id='default' AND payload_json::jsonb ->> 'runtime_input_id'=$1),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$2 AND type='session.error'),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$2
		  AND status IN ('queued','delivering','accepted','parked'))`, runtimeInputID, sessionID).Scan(
		&inboxStatus, &queueStatus, &errorKind, &sessionErrors, &liveInbox,
	); err != nil {
		t.Fatalf("read producer input exhaustion: %v", err)
	}
	if inboxStatus != "dead_lettered" || queueStatus != queue.StatusDeadLettered || errorKind != "runtime_delivery_exhausted" ||
		sessionErrors != 1 || liveInbox != 0 || len(sender.requests) != 0 {
		t.Fatalf("producer exhaustion = Inbox %s Queue %s/%s errors %d live %d Runtime calls %d",
			inboxStatus, queueStatus, errorKind, sessionErrors, liveInbox, len(sender.requests))
	}
	var record map[string]any
	if err := json.Unmarshal(attemptLog.Bytes(), &record); err != nil {
		t.Fatalf("decode Runtime delivery attempt log: %v", err)
	}
	keys := make([]string, 0, len(record))
	for key := range record {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	wantKeys := []string{
		"component", "event.kind", "finalization.disposition", "level", "msg", "operation",
		"preparation.error_kind", "queue.attempt", "queue.job.id", "queue.max_attempts",
		"runtime.input.id", "runtime.input.kind", "session.id", "thread.id", "time", "workspace.id",
	}
	if !reflect.DeepEqual(keys, wantKeys) || record["preparation.error_kind"] != "runtime_binding_unavailable" ||
		record["finalization.disposition"] != "rejected:runtime_delivery_exhausted" {
		t.Fatalf("Runtime delivery attempt log = %#v; want bounded attempt and finalization evidence", record)
	}
}
