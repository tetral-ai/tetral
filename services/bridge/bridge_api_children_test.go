package agentruntimebridge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestSelectForkEntriesUsesUserLedTurnBoundaries(t *testing.T) {
	entries := []json.RawMessage{
		json.RawMessage(`{"id":"u1"}`),
		json.RawMessage(`{"id":"a1"}`),
		json.RawMessage(`{"id":"c1"}`),
		json.RawMessage(`{"id":"u2"}`),
		json.RawMessage(`{"id":"a2"}`),
	}
	kinds := []string{"user", "assistant", "compaction", "runtime_notification", "assistant"}

	selected := selectForkEntries(entries, kinds, "1")
	if len(selected) != 2 || string(selected[0]) != `{"id":"u2"}` || string(selected[1]) != `{"id":"a2"}` {
		t.Fatalf("last user-led turn = %s; want u2,a2", mustJSON(selected))
	}
	all := selectForkEntries(entries, kinds, "2")
	if len(all) != len(entries) {
		t.Fatalf("two user-led turns selected %d entries; want %d", len(all), len(entries))
	}
}

func TestActorCreationBoundsMatchTheRuntimeContract(t *testing.T) {
	if !validActorTaskName(strings.Repeat("a", actorTaskNameMaxBytes)) {
		t.Fatal("exact 128-byte task name was rejected")
	}
	if validActorTaskName(strings.Repeat("a", actorTaskNameMaxBytes+1)) {
		t.Fatal("129-byte task name was accepted")
	}
	if !validActorTaskName(strings.Repeat("界", 42)) || validActorTaskName(strings.Repeat("界", 43)) {
		t.Fatal("task name bound was not applied to UTF-8 bytes")
	}
	if validActorTaskName(" padded ") || validActorTaskName(string([]byte{0xff})) {
		t.Fatal("non-canonical task name was accepted")
	}
	for _, value := range []string{"none", "all", "1", "1000"} {
		if !validForkTurns(value) {
			t.Fatalf("valid fork_turns %q was rejected", value)
		}
	}
	for _, value := range []string{"", "0", "01", "1001", "-1", "1.0"} {
		if validForkTurns(value) {
			t.Fatalf("invalid fork_turns %q was accepted", value)
		}
	}
}

func TestActorBoundaryDiagnosticsAreBoundedAndFailOpen(t *testing.T) {
	scope := bridgeAPIScope("sesn_actor_diagnostic", "thr_actor_diagnostic", "bind_actor_diagnostic", 1, "pod_actor_diagnostic")
	logActorBoundaryRejected(
		slog.New(panicSlogHandler{}), scope, "create_subagent_thread", strings.Repeat("x", 129), "validate",
		status.Error(codes.InvalidArgument, "private rejection detail"),
	)

	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	logActorBoundaryRejected(
		logger, scope, "create_subagent_thread", strings.Repeat("x", 129), "validate",
		status.Error(codes.InvalidArgument, "private rejection detail"),
	)
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatalf("decode actor diagnostic: %v", err)
	}
	if record["event.kind"] != "actor_boundary_rejected" || record["operation.id"] != "invalid" ||
		record["phase"] != "validate" || record["reason"] != codes.InvalidArgument.String() {
		t.Fatalf("actor diagnostic = %#v; want bounded operation identity and stable phase/reason", record)
	}
	if strings.Contains(output.String(), "private rejection detail") || strings.Contains(output.String(), strings.Repeat("x", 129)) {
		t.Fatalf("actor diagnostic leaked rejected input: %s", output.String())
	}
}

func TestAdmitChildInterruptAssignsDurableControlOperationIdentity(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_control_operation_identity"
		parentID  = "thr_control_operation_parent"
		childID   = "thr_control_operation_child"
		sourceID  = "evt_control_operation_source"
		bindingID = "bind_control_operation_identity"
		podUID    = "pod_control_operation_identity"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, sourceID, 1, "agent.tool_use",
		`{"type":"agent.tool_use","name":"close_agent","input":{"task_name":"task_`+childID+`"}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET visibility='public' WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, sourceID); err != nil {
		t.Fatalf("make control source public: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID)
	request := &bridgev1.AdmitChildInterruptRequest{Scope: scope, SourceToolUseEventId: sourceID}
	first, err := store.AdmitChildInterrupt(context.Background(), request)
	if err != nil {
		t.Fatalf("first AdmitChildInterrupt: %v", err)
	}
	operationID := first.GetCommitted().GetControlOperationId()
	if operationID == "" || operationID == sourceID {
		t.Fatalf("control operation id = %q; want a distinct Bridge-owned identity", operationID)
	}
	replay, err := store.AdmitChildInterrupt(context.Background(), request)
	if err != nil {
		t.Fatalf("replayed AdmitChildInterrupt: %v", err)
	}
	if replay.GetDuplicate().GetControlOperationId() != operationID {
		t.Fatalf("replayed control operation id = %q; want %q", replay.GetDuplicate().GetControlOperationId(), operationID)
	}
	if _, err := store.AwaitChildInterrupt(context.Background(), &bridgev1.AwaitChildInterruptRequest{Scope: scope, ControlOperationId: sourceID}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("AwaitChildInterrupt accepted source Tool identity: %v", err)
	}
	if _, err := store.AwaitChildInterrupt(context.Background(), &bridgev1.AwaitChildInterruptRequest{Scope: scope, ControlOperationId: operationID}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("AwaitChildInterrupt durable control lookup = %v; want pending census", err)
	}
}

func TestPostgreSQLDeliverInterAgentMailIsAtomicAcrossGeneratedGRPCAndConcurrentReplay(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_atomic_agent_mail"
		parentID  = "thr_atomic_agent_mail_parent"
		childID   = "thr_atomic_agent_mail_child"
		sourceID  = "evt_atomic_agent_mail_source"
		bindingID = "bind_atomic_agent_mail"
		podUID    = "pod_atomic_agent_mail"
		content   = "run the isolated verification"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, sourceID, 1, "agent.tool_use",
		`{"type":"agent.tool_use","name":"send_message","input":{"task_name":"task_`+childID+`","message":"`+content+`"}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET visibility='public'
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, sourceID); err != nil {
		t.Fatalf("make mail Tool source public: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC) }
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	RegisterBridgeAPI(server, store)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///atomic-agent-mail", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatalf("dial generated Bridge client: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := bridgev1.NewAgentRuntimeBridgeServiceClient(connection)
	request := &bridgev1.DeliverInterAgentMailRequest{
		Scope:      bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID),
		DeliveryId: agentMailDeliveryID(sourceID, childID), TargetThreadId: childID,
		SourceToolUseEventId: sourceID, Content: content,
	}

	type result struct {
		response *bridgev1.DeliverInterAgentMailResponse
		err      error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	for range 2 {
		go func() {
			ready.Done()
			<-start
			response, err := client.DeliverInterAgentMail(context.Background(), request)
			results <- result{response: response, err: err}
		}()
	}
	ready.Wait()
	close(start)
	committed, duplicate := 0, 0
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent generated DeliverInterAgentMail: %v", result.err)
		}
		if result.response.GetCommitted() != nil {
			committed++
		}
		if result.response.GetDuplicate() != nil {
			duplicate++
		}
	}
	if committed != 1 || duplicate != 1 {
		t.Fatalf("concurrent outcomes committed/duplicate = %d/%d; want 1/1", committed, duplicate)
	}
	replay, err := client.DeliverInterAgentMail(context.Background(), request)
	if err != nil || replay.GetDuplicate() == nil {
		t.Fatalf("lost-ACK replay = %#v/%v; want duplicate", replay, err)
	}
	var sent, received, inbox, queued, operation int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_sent'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_received'),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND payload_json::jsonb->>'runtime_input_id'=$2),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND operation=$3 AND idempotency_key=$4)`,
		sessionID, completionRuntimeInputID(request.GetDeliveryId()), bridgeOpDeliverInterAgentMail, request.GetDeliveryId(),
	).Scan(&sent, &received, &inbox, &queued, &operation); err != nil {
		t.Fatalf("read atomic mail state: %v", err)
	}
	if sent != 1 || received != 1 || inbox != 1 || queued != 1 || operation != 1 {
		t.Fatalf("atomic mail rows sent/received/inbox/queue/receipt = %d/%d/%d/%d/%d; want all one", sent, received, inbox, queued, operation)
	}
}

func TestPostgreSQLDeliverInterAgentMailQueueFailureRollsBackAllMailState(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_atomic_agent_mail_rollback"
		parentID  = "thr_atomic_agent_mail_rollback_parent"
		childID   = "thr_atomic_agent_mail_rollback_child"
		sourceID  = "evt_atomic_agent_mail_rollback_source"
		bindingID = "bind_atomic_agent_mail_rollback"
		podUID    = "pod_atomic_agent_mail_rollback"
		content   = "this delivery must roll back"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, sourceID, 1, "agent.tool_use",
		`{"type":"agent.tool_use","name":"send_message","input":{"task_name":"task_`+childID+`","message":"`+content+`"}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET visibility='public'
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, sourceID); err != nil {
		t.Fatalf("make mail Tool source public: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `CREATE FUNCTION fail_atomic_agent_mail_queue_birth() RETURNS trigger AS $$
		BEGIN RAISE EXCEPTION 'injected atomic agent mail Queue failure'; END; $$ LANGUAGE plpgsql;
		CREATE TRIGGER fail_atomic_agent_mail_queue_birth BEFORE INSERT ON queue_jobs
		FOR EACH ROW WHEN (NEW.kind = 'runtime_input') EXECUTE FUNCTION fail_atomic_agent_mail_queue_birth()`); err != nil {
		t.Fatalf("install Queue failure: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	deliveryID := agentMailDeliveryID(sourceID, childID)
	if _, err := store.DeliverInterAgentMail(context.Background(), &bridgev1.DeliverInterAgentMailRequest{
		Scope: bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID), DeliveryId: deliveryID,
		TargetThreadId: childID, SourceToolUseEventId: sourceID, Content: content,
	}); err == nil {
		t.Fatal("DeliverInterAgentMail succeeded despite injected Queue failure")
	}
	var sent, received, inbox, queued, operation int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_sent'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_received'),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND payload_json::jsonb->>'runtime_input_id'=$2),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND operation=$3 AND idempotency_key=$4)`,
		sessionID, completionRuntimeInputID(deliveryID), bridgeOpDeliverInterAgentMail, deliveryID,
	).Scan(&sent, &received, &inbox, &queued, &operation); err != nil {
		t.Fatalf("read rolled-back atomic mail state: %v", err)
	}
	if sent != 0 || received != 0 || inbox != 0 || queued != 0 || operation != 0 {
		t.Fatalf("rolled-back mail rows sent/received/inbox/queue/receipt = %d/%d/%d/%d/%d; want all zero", sent, received, inbox, queued, operation)
	}
}

func TestPostgreSQLMarkChildThreadActiveDerivesTargetFromDurableResumeTool(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_durable_resume_target"
		parentID  = "thr_durable_resume_parent"
		childID   = "thr_durable_resume_child"
		sourceID  = "evt_durable_resume_source"
		bindingID = "bind_durable_resume"
		podUID    = "pod_durable_resume"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, sourceID, 1, "agent.tool_use",
		`{"type":"agent.tool_use","name":"resume_agent","input":{"task_name":"task_`+childID+`"}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET visibility='public'
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, sourceID); err != nil {
		t.Fatalf("make resume Tool source public: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads SET status='closed_for_runtime',closed_at='2026-01-01T00:00:00Z'
		WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, childID); err != nil {
		t.Fatalf("close resume target: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	request := &bridgev1.MarkChildThreadActiveRequest{
		Scope: bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID), SourceToolUseEventId: sourceID,
	}
	response, err := store.MarkChildThreadActive(context.Background(), request)
	if err != nil || response.GetCommitted().GetDisposition() != bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_RESUMED {
		t.Fatalf("MarkChildThreadActive durable target = %#v/%v; want committed resumed", response, err)
	}
	replay, err := store.MarkChildThreadActive(context.Background(), request)
	if err != nil || replay.GetDuplicate().GetDisposition() != bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_RESUMED {
		t.Fatalf("MarkChildThreadActive lost-ACK replay = %#v/%v; want duplicate resumed", replay, err)
	}
	var statusValue string
	var operationCount int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$2),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1
		 AND session_thread_id=$2 AND operation='mark_child_thread_active')`, sessionID, childID).Scan(&statusValue, &operationCount); err != nil {
		t.Fatalf("read resumed child: %v", err)
	}
	if statusValue != "idle" || operationCount != 1 {
		t.Fatalf("resumed child status/receipt = %s/%d; want idle/1", statusValue, operationCount)
	}

	for index, test := range []struct {
		status      string
		disposition bridgev1.ChildLifecycleDisposition
	}{
		{status: "failed", disposition: bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_PRESERVED_FAILED},
		{status: "terminated", disposition: bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_PRESERVED_TERMINATED},
	} {
		sourceID := fmt.Sprintf("evt_durable_resume_%s_source", test.status)
		seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, sourceID, int64(index+2), "agent.tool_use",
			`{"type":"agent.tool_use","name":"resume_agent","input":{"task_name":"task_`+childID+`"}}`)
		if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET visibility='public'
			WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, sourceID); err != nil {
			t.Fatalf("make %s resume Tool source public: %v", test.status, err)
		}
		if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads SET status=$3,closed_at='2026-01-01T00:00:00Z'
			WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, childID, test.status); err != nil {
			t.Fatalf("set resume target %s: %v", test.status, err)
		}
		terminalRequest := &bridgev1.MarkChildThreadActiveRequest{Scope: request.GetScope(), SourceToolUseEventId: sourceID}
		terminalResponse, err := store.MarkChildThreadActive(context.Background(), terminalRequest)
		if err != nil || terminalResponse.GetCommitted().GetDisposition() != test.disposition {
			t.Fatalf("MarkChildThreadActive %s target = %#v/%v; want committed %s", test.status, terminalResponse, err, test.disposition)
		}
		terminalReplay, err := store.MarkChildThreadActive(context.Background(), terminalRequest)
		if err != nil || terminalReplay.GetDuplicate().GetDisposition() != test.disposition {
			t.Fatalf("MarkChildThreadActive %s replay = %#v/%v; want duplicate %s", test.status, terminalReplay, err, test.disposition)
		}
		if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_threads
			WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, childID).Scan(&statusValue); err != nil {
			t.Fatalf("read preserved %s target: %v", test.status, err)
		}
		if statusValue != test.status {
			t.Fatalf("resume changed terminal child status = %q; want %q", statusValue, test.status)
		}
	}

	terminalSourceID := "evt_durable_resume_terminal_source"
	seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, terminalSourceID, 4, "agent.tool_use",
		`{"type":"agent.tool_use","name":"resume_agent","input":{"task_name":"task_`+childID+`"}}`)
	seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, "evt_durable_resume_terminal_result", 5, "agent.tool_result",
		`{"type":"agent.tool_result","tool_use_event_id":"`+terminalSourceID+`","result":{"status":"completed"}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET visibility='public'
		WHERE workspace_id='default' AND session_id=$1 AND event_id IN ($2,$3)`, sessionID, terminalSourceID, "evt_durable_resume_terminal_result"); err != nil {
		t.Fatalf("make terminal resume source public: %v", err)
	}
	terminalRequest := &bridgev1.MarkChildThreadActiveRequest{
		Scope: request.GetScope(), SourceToolUseEventId: terminalSourceID,
	}
	if _, err := store.MarkChildThreadActive(context.Background(), terminalRequest); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("terminal resume Tool source err = %v; want FailedPrecondition", err)
	}
}

func TestPostgreSQLAdmitApprovalReviewInputSerializesConcurrentReplayWithoutQueue(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID  = "sesn_reviewer_admission_replay"
		parentID   = "thr_reviewer_admission_parent"
		reviewerID = "thr_reviewer_admission_target"
		reviewID   = "arvw_reviewer_admission"
		bindingID  = "bind_reviewer_admission"
		podUID     = "pod_reviewer_admission"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIInternalReviewerThread(t, admin, "default", sessionID, parentID, reviewerID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	request := &bridgev1.AdmitApprovalReviewInputRequest{
		Scope: bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID), ReviewerThreadId: reviewerID, ReviewId: reviewID,
	}

	responses := make([]*bridgev1.AdmitApprovalReviewInputResponse, 2)
	errorsSeen := make([]error, 2)
	start := make(chan struct{})
	var wait sync.WaitGroup
	for index := range responses {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			responses[index], errorsSeen[index] = store.AdmitApprovalReviewInput(context.Background(), request)
		}()
	}
	close(start)
	wait.Wait()

	committed, duplicate := 0, 0
	var runtimeInputID string
	for index, response := range responses {
		if errorsSeen[index] != nil {
			t.Fatalf("concurrent reviewer admission %d: %v", index, errorsSeen[index])
		}
		switch {
		case response.GetCommitted() != nil:
			committed++
			runtimeInputID = response.GetCommitted().GetRuntimeInputId()
		case response.GetDuplicate() != nil:
			duplicate++
			if runtimeInputID == "" {
				runtimeInputID = response.GetDuplicate().GetRuntimeInputId()
			}
		default:
			t.Fatalf("concurrent reviewer admission %d outcome = %#v", index, response)
		}
	}
	if committed != 1 || duplicate != 1 || runtimeInputID == "" {
		t.Fatalf("concurrent reviewer admissions committed/duplicate/id = %d/%d/%q; want 1/1/non-empty", committed, duplicate, runtimeInputID)
	}
	for index, response := range responses {
		gotID := response.GetCommitted().GetRuntimeInputId()
		if response.GetDuplicate() != nil {
			gotID = response.GetDuplicate().GetRuntimeInputId()
		}
		if gotID != runtimeInputID {
			t.Fatalf("concurrent reviewer admission %d runtime input id = %q; want %q", index, gotID, runtimeInputID)
		}
	}

	var inboxCount, queueCount int
	var statusValue, targetThreadID string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2),
		(SELECT session_thread_id FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND payload_json::jsonb->>'runtime_input_id'=$2)`,
		sessionID, runtimeInputID,
	).Scan(&inboxCount, &statusValue, &targetThreadID, &queueCount); err != nil {
		t.Fatalf("read concurrent reviewer admission authority: %v", err)
	}
	if inboxCount != 1 || statusValue != "accepted" || targetThreadID != reviewerID || queueCount != 0 {
		t.Fatalf("reviewer admission authority count/status/target/queue = %d/%q/%q/%d; want 1/accepted/%q/0",
			inboxCount, statusValue, targetThreadID, queueCount, reviewerID)
	}
}

func TestActorResponsesExposeOnlyOperationSpecificResults(t *testing.T) {
	created := &bridgev1.CreateSubagentThreadResponse{Outcome: &bridgev1.CreateSubagentThreadResponse_Committed{
		Committed: &bridgev1.CreateSubagentThreadCommitted{ChildThreadId: "thr_bridge_owned"},
	}}
	if created.GetCommitted().GetChildThreadId() != "thr_bridge_owned" {
		t.Fatal("create response lost Bridge-owned child identity")
	}
	delivered := &bridgev1.DeliverInterAgentMailResponse{Outcome: &bridgev1.DeliverInterAgentMailResponse_Committed{
		Committed: &bridgev1.DeliverInterAgentMailCommitted{},
	}}
	if delivered.GetCommitted() == nil || delivered.GetDuplicate() != nil {
		t.Fatal("delivery response is not a closed committed result")
	}
	resumed := &bridgev1.MarkChildThreadActiveResponse{Outcome: &bridgev1.MarkChildThreadActiveResponse_Committed{
		Committed: &bridgev1.MarkChildThreadActiveCommitted{Disposition: bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_RESUMED},
	}}
	if resumed.GetCommitted() == nil || resumed.GetDuplicate() != nil || resumed.GetStale() != nil {
		t.Fatal("resume response is not a closed echo-free result")
	}
	resolved := &bridgev1.ResolveChildThreadResponse{Outcome: &bridgev1.ResolveChildThreadResponse_Resolved{
		Resolved: &bridgev1.ResolveChildThreadResolved{Child: &bridgev1.ChildThreadFact{ChildThreadId: "thr_child"}},
	}}
	if resolved.GetResolved().GetChild().GetChildThreadId() != "thr_child" {
		t.Fatal("child read response lost its typed child fact")
	}
	listed := &bridgev1.ListChildThreadsResponse{Outcome: &bridgev1.ListChildThreadsResponse_Completed{
		Completed: &bridgev1.ListChildThreadsCompleted{Children: []*bridgev1.ChildThreadFact{{ChildThreadId: "thr_child"}}},
	}}
	if len(listed.GetCompleted().GetChildren()) != 1 {
		t.Fatal("child list response lost its typed child facts")
	}
	reviewerClosed := &bridgev1.CloseApprovalReviewerResponse{Outcome: &bridgev1.CloseApprovalReviewerResponse_Committed{
		Committed: &bridgev1.CloseApprovalReviewerCommitted{},
	}}
	if reviewerClosed.GetCommitted() == nil || reviewerClosed.GetDuplicate() != nil || reviewerClosed.GetStale() != nil {
		t.Fatal("reviewer close response is not a closed operation-specific result")
	}
}

func mustJSON(values []json.RawMessage) string {
	encoded, _ := json.Marshal(values)
	return string(encoded)
}
