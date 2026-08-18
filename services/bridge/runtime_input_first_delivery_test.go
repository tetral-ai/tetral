package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginefiles "github.com/tetral-ai/tetral/internal/files"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
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
	attachmentStore := blob.NewFakeBlobStore()
	seedBridgeAPIFileAttachment(t, admin, attachmentStore, "file_first_queued_image", "image.png", "image/png", "image")
	seedBridgeAPIFileAttachment(t, admin, attachmentStore, "file_first_queued_document", "notes.txt", "text/plain", "document")
	eventService := sessionevent.NewService(sessionevent.NewPostgreSQLStore(
		client,
		sessionevent.WithFileAttachmentValidator(enginefiles.NewPostgreSQLStore(client, attachmentStore)),
	))
	appended, err := eventService.AppendClientEvents(
		context.Background(),
		workspace.DefaultID,
		sessionID,
		"idem_first_queued_delivery",
		sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{
			Type: sessionevent.EventTypeUserMessage,
			Content: []sessionevent.ContentBlock{
				{Type: sessionevent.ContentBlockTypeImage, Source: &sessionevent.ContentSource{Type: sessionevent.ContentSourceTypeFile, FileID: "file_first_queued_image"}},
				{Type: sessionevent.ContentBlockTypeDocument, Source: &sessionevent.ContentSource{Type: sessionevent.ContentSourceTypeFile, FileID: "file_first_queued_document"}},
			},
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
	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
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
	if len(sender.requests) != 1 {
		t.Fatalf("Runtime requests = %#v; want one request for %s", sender.requests, runtimeInputID)
	}
	accepted, ok := sender.requests[0].(*agentruntimev1.AcceptInputRequest)
	if !ok || accepted.GetRuntimeInputId() != runtimeInputID {
		t.Fatalf("Runtime request = %#v; want AcceptInput for %s", sender.requests[0], runtimeInputID)
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

	apiStore := NewPostgreSQLBridgeAPIStore(client)
	apiStore.AttachmentBlobStore = attachmentStore
	apiStore.RuntimeBindingTokenHMACKey = []byte("attachment-hot-cold-composition-signing-key")
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIOpenDurableTurn(t, admin, scope, "rwrite_attachment_hot_cold_run")
	committed, err := apiStore.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope:          scope,
		RuntimeInputId: runtimeInputID,
	})
	if err != nil {
		t.Fatalf("commit delivered attachment input: %v", err)
	}
	application := committed.GetCommitted().GetContext()
	if len(application.GetAssignedContextSequences()) != 0 || len(application.GetPendingAttachmentJson()) != 2 {
		t.Fatalf("attachment receipt = %#v; want zero text sequences and two attachments", application)
	}
	loaded, err := apiStore.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope: scope,
	})
	if err != nil {
		t.Fatalf("load committed attachment input for cold composition: %v", err)
	}
	assertAttachmentHotColdComposition(t, accepted, committed, loaded.GetContextJson())
	assertAttachmentOnlyRuntimeInputFixture(t, accepted, committed)

	consumedFiles := make([]*bridgev1.FileAttachmentPair, 0, len(application.GetPendingAttachmentJson()))
	for _, raw := range application.GetPendingAttachmentJson() {
		var attachment bridgeLoadContextPendingAttachment
		if err := json.Unmarshal([]byte(raw), &attachment); err != nil || attachment.Origin.FileBacked == nil {
			t.Fatalf("decode committed file attachment %q: %v", raw, err)
		}
		consumedFiles = append(consumedFiles, &bridgev1.FileAttachmentPair{
			SourceEventId: attachment.Origin.FileBacked.SourceEventID,
			FileId:        attachment.Origin.FileBacked.FileID,
		})
	}
	seedBridgeAPIRequestStart(t, apiStore, scope, "rwrite_attachment_consumed_start", "mreq_attachment_consumed", "agent_provider_request", 0, consumedFiles...)
	if _, err := apiStore.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_attachment_consumed_end", ModelRequestId: "mreq_attachment_consumed",
		FinishReason: "stop", UsageJson: `{}`,
	}); err != nil {
		t.Fatalf("settle attachment-consuming request: %v", err)
	}
	consumedLoad, err := apiStore.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("cold load after attachment consumption: %v", err)
	}
	var consumedContext bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(consumedLoad.GetContextJson()), &consumedContext); err != nil {
		t.Fatalf("decode cold context after attachment consumption: %v", err)
	}
	if len(consumedContext.PendingAttachments) != 0 {
		t.Fatalf("consumed attachment cold load = %#v; want no pending media", consumedContext.PendingAttachments)
	}
	assertColdAttachmentRequestCount(t, accepted, consumedLoad.GetContextJson(), 0)
}

func assertAttachmentHotColdComposition(
	t *testing.T,
	accepted *agentruntimev1.AcceptInputRequest,
	committed *bridgev1.CommitInputsResponse,
	coldContextJSON string,
) {
	t.Helper()
	input := map[string]any{
		"acceptedInput": map[string]any{
			"workspaceId": accepted.GetWorkspaceId(), "sessionId": accepted.GetSessionId(),
			"sessionThreadId": accepted.GetSessionThreadId(), "bindingId": accepted.GetBindingId(),
			"bindingGeneration": accepted.GetBindingGeneration(), "targetPodUid": accepted.GetTargetPodUid(),
			"runtimeInputId": accepted.GetRuntimeInputId(), "inputOrder": accepted.GetInputOrder(),
			"kind": "messages", "contentJson": accepted.GetMessagesJson(),
		},
		"commitInputsResponse": map[string]any{"committed": map[string]any{"context": map[string]any{
			"assignedContextSequences": committed.GetCommitted().GetContext().GetAssignedContextSequences(),
			"pendingAttachmentJson":    committed.GetCommitted().GetContext().GetPendingAttachmentJson(),
		}}},
		"coldContextJson": coldContextJSON,
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("encode attachment hot/cold composition: %v", err)
	}
	inputPath := t.TempDir() + "/attachment-hot-cold.json"
	if err := os.WriteFile(inputPath, encoded, 0o600); err != nil {
		t.Fatalf("write attachment hot/cold composition: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/attachment-hot-cold-composition.ts", inputPath) //nolint:gosec // Fixed production composition fixture and test-owned input.
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run attachment hot/cold composition: %v: %s", err, output)
	}
	type requestProjection struct {
		RequestCount int               `json:"requestCount"`
		Context      []json.RawMessage `json:"context"`
		Attachments  []json.RawMessage `json:"attachments"`
	}
	var composed struct {
		Hot  requestProjection `json:"hot"`
		Cold requestProjection `json:"cold"`
	}
	if err := json.Unmarshal(output, &composed); err != nil {
		t.Fatalf("decode attachment hot/cold composition: %v: %s", err, output)
	}
	if composed.Hot.RequestCount != 1 || composed.Cold.RequestCount != 1 ||
		len(composed.Hot.Context) != 0 || len(composed.Cold.Context) != 0 ||
		len(composed.Hot.Attachments) != 2 || !reflect.DeepEqual(composed.Hot, composed.Cold) {
		t.Fatalf("attachment hot/cold composition = hot %#v cold %#v; want one identical attachment-only request", composed.Hot, composed.Cold)
	}
}

func assertColdAttachmentRequestCount(
	t *testing.T,
	accepted *agentruntimev1.AcceptInputRequest,
	coldContextJSON string,
	want int,
) {
	t.Helper()
	input := map[string]any{
		"acceptedInput": map[string]any{
			"workspaceId": accepted.GetWorkspaceId(), "sessionId": accepted.GetSessionId(),
			"sessionThreadId": accepted.GetSessionThreadId(), "bindingId": accepted.GetBindingId(),
			"bindingGeneration": accepted.GetBindingGeneration(), "targetPodUid": accepted.GetTargetPodUid(),
			"runtimeInputId": accepted.GetRuntimeInputId(), "inputOrder": accepted.GetInputOrder(),
			"kind": "messages", "contentJson": accepted.GetMessagesJson(),
		},
		"coldContextJson": coldContextJSON,
		"coldOnly":        true,
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("encode cold attachment composition: %v", err)
	}
	inputPath := t.TempDir() + "/attachment-cold.json"
	if err := os.WriteFile(inputPath, encoded, 0o600); err != nil {
		t.Fatalf("write cold attachment composition: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/attachment-hot-cold-composition.ts", inputPath) //nolint:gosec // Fixed production composition fixture and test-owned input.
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run cold attachment composition: %v: %s", err, output)
	}
	var composed struct {
		Cold struct {
			RequestCount int `json:"requestCount"`
		} `json:"cold"`
	}
	if err := json.Unmarshal(output, &composed); err != nil {
		t.Fatalf("decode cold attachment composition: %v: %s", err, output)
	}
	if composed.Cold.RequestCount != want {
		t.Fatalf("cold attachment Provider requests = %d; want %d", composed.Cold.RequestCount, want)
	}
}

func assertAttachmentOnlyRuntimeInputFixture(t *testing.T, accepted *agentruntimev1.AcceptInputRequest, committed *bridgev1.CommitInputsResponse) {
	t.Helper()
	pendingAttachmentJSON := append([]string(nil), committed.GetCommitted().GetContext().GetPendingAttachmentJson()...)
	for index, raw := range pendingAttachmentJSON {
		var attachment map[string]any
		if err := json.Unmarshal([]byte(raw), &attachment); err != nil {
			t.Fatalf("decode produced attachment %d: %v", index, err)
		}
		origin := attachment["origin"].(map[string]any)
		fileBacked := origin["fileBacked"].(map[string]any)
		fileBacked["sourceEventId"] = "sevt_attachment_input"
		normalized, err := json.Marshal(attachment)
		if err != nil {
			t.Fatalf("normalize produced attachment %d: %v", index, err)
		}
		pendingAttachmentJSON[index] = string(normalized)
	}
	actualJSON, err := json.Marshal(map[string]any{
		"acceptInput": map[string]any{
			"workspaceId": accepted.GetWorkspaceId(), "sessionId": accepted.GetSessionId(),
			"sessionThreadId": accepted.GetSessionThreadId(), "bindingId": accepted.GetBindingId(),
			"bindingGeneration": accepted.GetBindingGeneration(), "targetPodUid": accepted.GetTargetPodUid(),
			"runtimeInputId": "rin_attachment_input", "inputOrder": accepted.GetInputOrder(),
			"messagesJson": accepted.GetMessagesJson(),
		},
		"commitInputs": map[string]any{"committed": map[string]any{"context": map[string]any{
			"assignedContextSequences": committed.GetCommitted().GetContext().GetAssignedContextSequences(),
			"pendingAttachmentJson":    pendingAttachmentJSON,
		}}},
	})
	if err != nil {
		t.Fatalf("marshal produced attachment fixture: %v", err)
	}
	fixtureJSON, err := os.ReadFile(filepath.Join(repoRootFromBridgeTest(t), "testdata", "attachment-only-runtime-input.json"))
	if err != nil {
		t.Fatalf("read attachment Runtime input fixture: %v", err)
	}
	var actual, expected any
	if err := json.Unmarshal(actualJSON, &actual); err != nil {
		t.Fatalf("decode produced attachment fixture: %v", err)
	}
	if err := json.Unmarshal(fixtureJSON, &expected); err != nil {
		t.Fatalf("decode checked-in attachment fixture: %v", err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("attachment Runtime input fixture = %s; want %s", actualJSON, fixtureJSON)
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
	attachmentStore := blob.NewFakeBlobStore()
	seedBridgeAPIFileAttachment(t, admin, attachmentStore, "file_first_queued_exhaustion", "exhausted.png", "image/png", "exhausted")
	eventService := sessionevent.NewService(sessionevent.NewPostgreSQLStore(
		client,
		sessionevent.WithFileAttachmentValidator(enginefiles.NewPostgreSQLStore(client, attachmentStore)),
	))
	appended, err := eventService.AppendClientEvents(
		context.Background(), workspace.DefaultID, sessionID, "idem_first_queued_exhaustion",
		sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{
			Type: sessionevent.EventTypeUserMessage,
			Content: []sessionevent.ContentBlock{{
				Type:   sessionevent.ContentBlockTypeImage,
				Source: &sessionevent.ContentSource{Type: sessionevent.ContentSourceTypeFile, FileID: "file_first_queued_exhaustion"},
			}},
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
	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
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

	if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads
		SET status='closed_for_runtime', updated_at=now()
		WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, threadID); err != nil {
		t.Fatalf("close Thread after terminal media delivery: %v", err)
	}
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 2, podUID)
	apiStore := NewPostgreSQLBridgeAPIStore(client)
	apiStore.RuntimeBindingTokenHMACKey = []byte("attachment-terminal-cold-load-key-32")
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 2, podUID)
	loaded, err := apiStore.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("cold load terminal media input: %v", err)
	}
	var terminalContext bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loaded.GetContextJson()), &terminalContext); err != nil {
		t.Fatalf("decode terminal media cold context: %v", err)
	}
	if len(terminalContext.PendingAttachments) != 0 {
		t.Fatalf("terminal media cold load = %#v; want no pending attachments", terminalContext.PendingAttachments)
	}
	const (
		parentThreadID = "thr_first_queued_exhaustion_parent"
		childTaskName  = "terminal_media_worker"
		resumeSourceID = "evt_first_queued_exhaustion_resume"
	)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, threadID, parentThreadID)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads
		SET parent_thread_id=$2, role='subagent', task_name=$3, agent_type='worker', updated_at=now()
		WHERE workspace_id='default' AND session_id=$1 AND id=$4`, sessionID, parentThreadID, childTaskName, threadID); err != nil {
		t.Fatalf("convert terminal-media Thread to a resumable child: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads
		SET parent_thread_id=NULL, role='main', task_name=NULL, agent_type='default', updated_at=now()
		WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, parentThreadID); err != nil {
		t.Fatalf("promote terminal-media parent Thread: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET main_thread_id=$2
		WHERE workspace_id='default' AND id=$1`, sessionID, parentThreadID); err != nil {
		t.Fatalf("point Session at terminal-media parent Thread: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, parentThreadID, resumeSourceID, 1, "agent.tool_use",
		`{"type":"agent.tool_use","name":"resume_agent","input":{"task_name":"`+childTaskName+`"},"evaluated_permission":"allow"}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET visibility='public', session_visible=true
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, resumeSourceID); err != nil {
		t.Fatalf("authorize terminal-media resume source: %v", err)
	}
	assertClosedThreadResumeWithoutPhantomAttachment(t, runtime, map[string]any{
		"workspaceId": "default", "sessionId": sessionID, "parentThreadId": parentThreadID,
		"childThreadId": threadID, "childTaskName": childTaskName, "bindingId": bindingID,
		"bindingGeneration": 2, "targetPodUid": podUID, "sourceToolUseEventId": resumeSourceID,
	})
}

func assertClosedThreadResumeWithoutPhantomAttachment(t *testing.T, runtime *sql.DB, input map[string]any) {
	t.Helper()
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("closed-thread-resume-composition-key")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for closed Thread resume composition: %v", err)
	}
	server := grpc.NewServer()
	RegisterBridgeAPI(server, store)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	input["address"] = listener.Addr().String()
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("encode closed Thread resume input: %v", err)
	}
	inputPath := filepath.Join(t.TempDir(), "closed-thread-resume.json")
	if err := os.WriteFile(inputPath, encoded, 0o600); err != nil {
		t.Fatalf("write closed Thread resume input: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/closed-thread-resume-composition.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run closed Thread through Runtime resume owner: %v: %s", err, output)
	}
	var result struct {
		Result struct {
			Type string `json:"type"`
		} `json:"result"`
		Inspected struct {
			OK       bool   `json:"ok"`
			Observed bool   `json:"observed"`
			Status   string `json:"status"`
		} `json:"inspected"`
		ProviderRequests int `json:"providerRequests"`
		RuntimeEvents    int `json:"runtimeEvents"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode closed Thread resume result: %v: %s", err, output)
	}
	if result.Result.Type != "completed" || !result.Inspected.OK || !result.Inspected.Observed ||
		result.Inspected.Status != "idle" || result.ProviderRequests != 0 || result.RuntimeEvents != 0 {
		t.Fatalf("closed Thread resume = %#v; want completed resident idle with no Provider request or Runtime write", result)
	}
}
