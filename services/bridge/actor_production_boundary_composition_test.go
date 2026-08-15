package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func startActorProductionBridge(t *testing.T, runtime *sql.DB) bridgev1.AgentRuntimeBridgeServiceClient {
	t.Helper()
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("actor-production-composition-key")
	listener := bufconn.Listen(2 * 1024 * 1024)
	server := grpc.NewServer()
	RegisterBridgeAPI(server, store)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///actor-production-composition",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	if err != nil {
		t.Fatalf("dial generated actor Bridge client: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return bridgev1.NewAgentRuntimeBridgeServiceClient(connection)
}

func TestSubagentMailColdLoadCloseAndResumeAcrossGeneratedGRPCAndPostgreSQL(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_actor_production"
		parentID       = "thr_actor_production_parent"
		bindingID      = "bind_actor_production"
		podUID         = "pod_actor_production"
		spawnSourceID  = "evt_actor_production_spawn"
		spawnRequestID = "mreq_actor_production_spawn"
		taskName       = "durable-child"
		mailSourceID   = "evt_actor_production_mail"
		mailContent    = "inspect the target-owned durable envelope"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, spawnSourceID, 1, "agent.tool_use",
		`{"type":"agent.tool_use","name":"spawn_agent","input":{"task_name":"`+taskName+`","agent_type":"worker","fork_turns":"all"}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events
		SET visibility='public',session_visible=true,model_request_id=$2
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$3`, sessionID, spawnRequestID, spawnSourceID); err != nil {
		t.Fatalf("authorize durable spawn source: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, parentID, spawnRequestID, spawnSourceID, "call_actor_production_spawn", "spawn_agent")
	seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, "evt_actor_production_spawn_end", 2, "span.model_request_end",
		`{"type":"span.model_request_end","model_request_id":"`+spawnRequestID+`","is_error":false}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET model_request_id=$2
		WHERE workspace_id='default' AND session_id=$1 AND event_id='evt_actor_production_spawn_end'`, sessionID, spawnRequestID); err != nil {
		t.Fatalf("seal durable spawn request: %v", err)
	}

	client := startActorProductionBridge(t, runtime)
	parentScope := bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID)
	spawnRequest := &bridgev1.CreateSubagentThreadRequest{
		Scope: parentScope, SourceToolUseEventId: spawnSourceID, TaskName: taskName, AgentType: "worker", ForkTurns: "all",
	}
	spawned, err := client.CreateSubagentThread(context.Background(), spawnRequest)
	if err != nil || spawned.GetCommitted().GetChildThreadId() == "" {
		t.Fatalf("create subagent through generated gRPC = %#v/%v", spawned, err)
	}
	childID := spawned.GetCommitted().GetChildThreadId()
	replayed, err := client.CreateSubagentThread(context.Background(), spawnRequest)
	if err != nil || replayed.GetDuplicate().GetChildThreadId() != childID {
		t.Fatalf("replay generated subagent creation = %#v/%v; want %s", replayed, err, childID)
	}
	var prefixParent, prefixBoundary, prefixEntries string
	if err := admin.QueryRowContext(context.Background(), `SELECT parent_thread_id,parent_boundary_event_id,entries_json
		FROM session_thread_context_prefixes WHERE workspace_id='default' AND session_id=$1 AND child_thread_id=$2`, sessionID, childID).
		Scan(&prefixParent, &prefixBoundary, &prefixEntries); err != nil {
		t.Fatalf("read Bridge-selected subagent prefix: %v", err)
	}
	if prefixParent != parentID || prefixBoundary != spawnSourceID || prefixEntries == "[]" {
		t.Fatalf("subagent prefix parent/boundary/entries = %s/%s/%s", prefixParent, prefixBoundary, prefixEntries)
	}

	seedActorSourceEvent(t, admin, sessionID, parentID, mailSourceID, "agent.tool_use",
		`{"type":"agent.tool_use","name":"send_message","input":{"task_name":"`+taskName+`","message":"`+mailContent+`"}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET visibility='public',session_visible=true
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, mailSourceID); err != nil {
		t.Fatalf("authorize durable mail source: %v", err)
	}
	deliveryID := agentMailDeliveryID(mailSourceID, childID)
	delivered, err := client.DeliverInterAgentMail(context.Background(), &bridgev1.DeliverInterAgentMailRequest{
		Scope: parentScope, DeliveryId: deliveryID, TargetThreadId: childID, SourceToolUseEventId: mailSourceID, Content: mailContent,
	})
	if err != nil || delivered.GetCommitted() == nil {
		t.Fatalf("deliver inter-agent mail through generated gRPC = %#v/%v", delivered, err)
	}
	childScope := bridgeAPIScope(sessionID, childID, bindingID, 1, podUID)
	loaded, err := client.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: childScope, RuntimeInputId: "cold_load:actor-production"})
	if err != nil || (loaded.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED && loaded.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE) {
		t.Fatalf("cold-load target-owned mail through generated gRPC = %#v/%v", loaded, err)
	}
	if !strings.Contains(loaded.GetContextJson(), deliveryID) || !strings.Contains(loaded.GetContextJson(), mailContent) {
		t.Fatalf("cold target context does not contain durable delivery/content: %s", loaded.GetContextJson())
	}

	closeSourceID := "evt_actor_production_close"
	seedActorSourceEvent(t, admin, sessionID, parentID, closeSourceID, "agent.tool_use",
		`{"type":"agent.tool_use","name":"close_agent","model_tool_call_id":"call_actor_production_close","input":{"task_name":"`+taskName+`"}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET visibility='public',session_visible=true,model_request_id='mreq_actor_production_close',projection_json='{"model_tool_call_id":"call_actor_production_close"}'
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, closeSourceID); err != nil {
		t.Fatalf("authorize durable close source: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, parentID, "mreq_actor_production_close", closeSourceID, "call_actor_production_close", "close_agent")
	admitted, err := client.AdmitChildInterrupt(context.Background(), &bridgev1.AdmitChildInterruptRequest{Scope: parentScope, SourceToolUseEventId: closeSourceID})
	if err != nil || admitted.GetCommitted().GetControlOperationId() == "" {
		t.Fatalf("admit durable child close control = %#v/%v", admitted, err)
	}
	controlID := admitted.GetCommitted().GetControlOperationId()
	var interruptInputID, interruptEventIDsJSON string
	var interruptSequenceFrom, interruptSequenceTo int64
	interruptInputErr := admin.QueryRowContext(context.Background(), `SELECT runtime_input_id,event_ids_json,sequence_from,sequence_to FROM session_runtime_inbox
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND input_kind='interrupt_control'
		ORDER BY created_at DESC LIMIT 1`, sessionID, childID).Scan(&interruptInputID, &interruptEventIDsJSON, &interruptSequenceFrom, &interruptSequenceTo)
	if interruptInputErr != nil && !errors.Is(interruptInputErr, sql.ErrNoRows) {
		t.Fatalf("read admitted child interrupt input: %v", interruptInputErr)
	}
	if interruptInputErr == nil {
		var interruptEventIDs []string
		if err := json.Unmarshal([]byte(interruptEventIDsJSON), &interruptEventIDs); err != nil {
			t.Fatalf("decode admitted child interrupt event IDs: %v", err)
		}
		deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
		deliveryStore.TargetResolver = &recordingRuntimeTargetResolver{binding: runtimeBindingForDelivery{
			BindingID: bindingID, BindingGeneration: 1, Namespace: "engine", PodName: "runtime-actor-production",
			PodUID: podUID, PodIP: "127.0.0.1",
		}}
		plan, prepareErr := deliveryStore.PrepareRuntimeCommand(context.Background(), RuntimeJob{
			Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: sessionID, SessionThreadID: childID,
			RuntimeInputID: interruptInputID, InputKind: "interrupt_control", EventIDs: interruptEventIDs,
			SequenceFrom: interruptSequenceFrom, SequenceTo: interruptSequenceTo,
		})
		if prepareErr != nil || plan.Interrupt == nil {
			t.Fatalf("prepare admitted child interrupt through production delivery store = %#v/%v", plan, prepareErr)
		}
		committedInterrupt, err := client.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
			Scope: childScope, RuntimeInputId: interruptInputID, Disposition: bridgev1.RuntimeInputDisposition_RUNTIME_INPUT_DISPOSITION_COMMIT,
		})
		if err != nil || committedInterrupt.GetCommitted().GetInterrupt() == nil {
			t.Fatalf("commit child interrupt through generated gRPC = %#v/%v", committedInterrupt, err)
		}
	}
	awaited, err := client.AwaitChildInterrupt(context.Background(), &bridgev1.AwaitChildInterruptRequest{Scope: parentScope, ControlOperationId: controlID})
	if err != nil || len(awaited.GetCompleted().GetTargets()) != 1 {
		t.Fatalf("await child interrupt through generated gRPC = %#v/%v", awaited, err)
	}
	closed, err := client.CloseChildControl(context.Background(), &bridgev1.CloseChildControlRequest{Scope: parentScope, ControlOperationId: controlID})
	if err != nil || len(closed.GetCommitted().GetChildren()) != 1 {
		t.Fatalf("close child through admitted operation = %#v/%v", closed, err)
	}
	closeReplay, err := client.CloseChildControl(context.Background(), &bridgev1.CloseChildControlRequest{Scope: parentScope, ControlOperationId: controlID})
	if err != nil || len(closeReplay.GetDuplicate().GetChildren()) != 1 {
		t.Fatalf("lost-ACK child close replay = %#v/%v", closeReplay, err)
	}
	settledClose, err := client.SettleToolResult(context.Background(), &bridgev1.SettleToolResultRequest{
		Scope: parentScope, Settlement: bridgeCompletedToolSettlementForTest(closeSourceID, "child closed"),
	})
	if err != nil || settledClose.GetCommitted() == nil {
		t.Fatalf("settle child close source through dedicated Tool result = %#v/%v", settledClose, err)
	}

	resumeSourceID := "evt_actor_production_resume"
	seedActorSourceEvent(t, admin, sessionID, parentID, resumeSourceID, "agent.tool_use",
		`{"type":"agent.tool_use","name":"resume_agent","input":{"task_name":"`+taskName+`"}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET visibility='public',session_visible=true
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, resumeSourceID); err != nil {
		t.Fatalf("authorize durable resume source: %v", err)
	}
	resumed, err := client.MarkChildThreadActive(context.Background(), &bridgev1.MarkChildThreadActiveRequest{Scope: parentScope, SourceToolUseEventId: resumeSourceID})
	if err != nil || resumed.GetCommitted() == nil {
		t.Fatalf("resume child through generated gRPC = %#v/%v", resumed, err)
	}
	var finalStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_threads
		WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, childID).Scan(&finalStatus); err != nil {
		t.Fatalf("read resumed child: %v", err)
	}
	if finalStatus != "idle" {
		t.Fatalf("resumed child status = %s; want idle", finalStatus)
	}
}

func seedActorSourceEvent(t *testing.T, db *sql.DB, sessionID, threadID, eventID, eventType, payload string) {
	t.Helper()
	var sequence int64
	if err := db.QueryRowContext(context.Background(), `SELECT COALESCE(MAX(sequence),0)+1 FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2`, sessionID, threadID).Scan(&sequence); err != nil {
		t.Fatalf("allocate actor source event sequence: %v", err)
	}
	seedBridgeAPIEvent(t, db, "default", sessionID, threadID, eventID, sequence, eventType, payload)
}

func TestReviewerTrunkSuccessionAndSidecarReplayAcrossGeneratedGRPCAndPostgreSQL(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_reviewer_production"
		parentID  = "thr_reviewer_production_parent"
		bindingID = "bind_reviewer_production"
		podUID    = "pod_reviewer_production"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	client := startActorProductionBridge(t, runtime)
	scope := bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID)

	firstRequest := &bridgev1.EnsureApprovalReviewerTrunkRequest{Scope: scope, EnsureOperationId: "ensure_reviewer_manager_1"}
	first, err := client.EnsureApprovalReviewerTrunk(context.Background(), firstRequest)
	if err != nil || first.GetCommitted().GetReviewerThreadId() == "" {
		t.Fatalf("ensure first reviewer trunk = %#v/%v", first, err)
	}
	firstID := first.GetCommitted().GetReviewerThreadId()
	firstReplay, err := client.EnsureApprovalReviewerTrunk(context.Background(), firstRequest)
	if err != nil || firstReplay.GetDuplicate().GetReviewerThreadId() != firstID {
		t.Fatalf("replay first reviewer trunk = %#v/%v", firstReplay, err)
	}
	second, err := client.EnsureApprovalReviewerTrunk(context.Background(), &bridgev1.EnsureApprovalReviewerTrunkRequest{
		Scope: scope, EnsureOperationId: "ensure_reviewer_manager_2",
	})
	if err != nil || second.GetCommitted().GetReviewerThreadId() == "" || second.GetCommitted().GetReviewerThreadId() == firstID {
		t.Fatalf("new-manager reviewer trunk succession = %#v/%v", second, err)
	}
	secondID := second.GetCommitted().GetReviewerThreadId()
	var firstTrunk, secondTrunk bool
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT is_trunk FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$2),
		(SELECT is_trunk FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$3)`, sessionID, firstID, secondID).
		Scan(&firstTrunk, &secondTrunk); err != nil {
		t.Fatalf("read reviewer trunk succession: %v", err)
	}
	if firstTrunk || !secondTrunk {
		t.Fatalf("reviewer trunk flags old/new = %t/%t; want false/true", firstTrunk, secondTrunk)
	}

	sidecarRequest := &bridgev1.EnsureApprovalReviewerSidecarRequest{Scope: scope, ReviewId: "review_production_1"}
	sidecar, err := client.EnsureApprovalReviewerSidecar(context.Background(), sidecarRequest)
	if err != nil || sidecar.GetCommitted().GetReviewerThreadId() == "" {
		t.Fatalf("ensure review-keyed sidecar = %#v/%v", sidecar, err)
	}
	sidecarID := sidecar.GetCommitted().GetReviewerThreadId()
	sidecarReplay, err := client.EnsureApprovalReviewerSidecar(context.Background(), sidecarRequest)
	if err != nil || sidecarReplay.GetDuplicate().GetReviewerThreadId() != sidecarID {
		t.Fatalf("replay review-keyed sidecar = %#v/%v", sidecarReplay, err)
	}
	var prefixSource string
	if err := admin.QueryRowContext(context.Background(), `SELECT parent_thread_id FROM session_thread_context_prefixes
		WHERE workspace_id='default' AND session_id=$1 AND child_thread_id=$2`, sessionID, sidecarID).Scan(&prefixSource); err != nil {
		t.Fatalf("read sidecar durable prefix source: %v", err)
	}
	if prefixSource != secondID {
		t.Fatalf("sidecar prefix source = %s; want current trunk %s", prefixSource, secondID)
	}
	admitted, err := client.AdmitApprovalReviewInput(context.Background(), &bridgev1.AdmitApprovalReviewInputRequest{
		Scope: scope, ReviewerThreadId: sidecarID, ReviewId: sidecarRequest.GetReviewId(),
	})
	if err != nil || admitted.GetCommitted().GetRuntimeInputId() == "" {
		t.Fatalf("admit review-keyed sidecar input = %#v/%v", admitted, err)
	}
	reviewInputID := admitted.GetCommitted().GetRuntimeInputId()
	admissionReplay, err := client.AdmitApprovalReviewInput(context.Background(), &bridgev1.AdmitApprovalReviewInputRequest{
		Scope: scope, ReviewerThreadId: sidecarID, ReviewId: sidecarRequest.GetReviewId(),
	})
	if err != nil || admissionReplay.GetDuplicate().GetRuntimeInputId() != reviewInputID {
		t.Fatalf("replay review-keyed input admission = %#v/%v", admissionReplay, err)
	}
	reviewerScope := bridgeAPIScope(sessionID, sidecarID, bindingID, 1, podUID)
	committedInput, err := client.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope: reviewerScope, RuntimeInputId: reviewInputID,
		Disposition:        bridgev1.RuntimeInputDisposition_RUNTIME_INPUT_DISPOSITION_COMMIT,
		ApprovalReviewText: []string{"review the bounded approval evidence"},
	})
	if err != nil || committedInput.GetCommitted().GetContext() == nil {
		t.Fatalf("commit approval reviewer input through generated gRPC = %#v/%v", committedInput, err)
	}
	seedActorSourceEvent(t, admin, sessionID, sidecarID, "evt_reviewer_production_decision", "approval_review.decision",
		`{"type":"approval_review.decision","review_id":"`+sidecarRequest.GetReviewId()+`","decision":"approved"}`)
	closed, err := client.CloseApprovalReviewer(context.Background(), &bridgev1.CloseApprovalReviewerRequest{
		Scope: scope, ReviewerThreadId: sidecarID, ReviewId: sidecarRequest.GetReviewId(),
	})
	if err != nil || closed.GetCommitted() == nil {
		t.Fatalf("close reviewer sidecar through generated gRPC = %#v/%v", closed, err)
	}
	closeReplay, err := client.CloseApprovalReviewer(context.Background(), &bridgev1.CloseApprovalReviewerRequest{
		Scope: scope, ReviewerThreadId: sidecarID, ReviewId: sidecarRequest.GetReviewId(),
	})
	if err != nil || closeReplay.GetDuplicate() == nil {
		t.Fatalf("lost-ACK reviewer close replay = %#v/%v", closeReplay, err)
	}

	var operationCount int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_bridge_operations
		WHERE workspace_id='default' AND session_id=$1 AND operation='create_child_thread'`, sessionID).Scan(&operationCount); err != nil {
		t.Fatalf("count reviewer ensure operations: %v", err)
	}
	if operationCount != 3 {
		t.Fatalf("durable reviewer ensure operation count = %d; want two trunks plus one sidecar", operationCount)
	}
}
