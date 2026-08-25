package agentruntimebridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

type reviewerRuntimeCompositionOutput struct {
	TrunkResult struct {
		Type    string `json:"type"`
		Outcome string `json:"outcome"`
	} `json:"trunkResult"`
	Decision struct {
		Type    string `json:"type"`
		Outcome string `json:"outcome"`
	} `json:"decision"`
	Failure struct {
		Type string `json:"type"`
	} `json:"failure"`
	CancellationSettled bool `json:"cancellationSettled"`
	ProviderRequests    int  `json:"providerRequests"`
	Creations           []struct {
		ReviewID string `json:"reviewId"`
		IsTrunk  bool   `json:"isTrunk"`
	} `json:"creations"`
	HotStateBeforeDispose struct {
		Executions         []any    `json:"executions"`
		EphemeralReviewIDs []string `json:"ephemeralReviewIds"`
	} `json:"hotStateBeforeDispose"`
	ManagerDisposed bool `json:"managerDisposed"`
}

func TestPostgreSQLReviewerRunExitClosesWithExactDurableAuthority(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_reviewer_runtime_composition"
		parentID  = "thr_reviewer_runtime_composition_parent"
		bindingID = "bind_reviewer_runtime_composition"
		podUID    = "pod_reviewer_runtime_composition"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("reviewer-runtime-composition-key")
	parentScope := bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID)
	parentStart := seedBridgeAPIRequestStart(t, store, parentScope, "rwrite_reviewer_composition_parent_start", "mreq_reviewer_composition_parent", requestKindAgentProviderRequest, 0)
	parentBoundaryEventID := parentStart.GetCommitted().GetEventId()
	if parentBoundaryEventID == "" {
		t.Fatalf("Reviewer parent Request Start = %#v; want committed Event", parentStart)
	}
	var callsMu sync.Mutex
	var closeRequests []*bridgev1.CloseApprovalReviewerRequest
	var admissionCalls int
	lostAdmissionACK := true
	lostDecisionCloseACK := true
	interceptor := func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if closeRequest, ok := request.(*bridgev1.CloseApprovalReviewerRequest); ok &&
			closeRequest.GetSettlementKind() == bridgev1.ApprovalReviewerCloseSettlementKind_APPROVAL_REVIEWER_CLOSE_SETTLEMENT_KIND_INTERRUPTED_REQUEST {
			if _, updateErr := admin.ExecContext(ctx, `UPDATE session_events
				SET payload_json = (payload_json::jsonb - 'error_kind' - 'finish_reason')::text
				WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND event_id=$3`,
				sessionID, closeRequest.GetReviewerThreadId(), closeRequest.GetSettlementEventId()); updateErr != nil {
				return nil, status.Error(codes.Internal, "strip Reviewer payload details for ownership proof")
			}
		}
		response, err := handler(ctx, request)
		callsMu.Lock()
		defer callsMu.Unlock()
		if info.FullMethod == bridgev1.AgentRuntimeBridgeService_AdmitApprovalReviewInput_FullMethodName {
			admissionCalls++
			if lostAdmissionACK && err == nil {
				lostAdmissionACK = false
				return nil, status.Error(codes.Unavailable, "simulated Reviewer admission ACK loss")
			}
		}
		if info.FullMethod == bridgev1.AgentRuntimeBridgeService_CloseApprovalReviewer_FullMethodName {
			closeRequest, ok := request.(*bridgev1.CloseApprovalReviewerRequest)
			if ok {
				closeRequests = append(closeRequests, proto.Clone(closeRequest).(*bridgev1.CloseApprovalReviewerRequest))
				if lostDecisionCloseACK && err == nil && closeRequest.GetSettlementKind() == bridgev1.ApprovalReviewerCloseSettlementKind_APPROVAL_REVIEWER_CLOSE_SETTLEMENT_KIND_DECISION {
					lostDecisionCloseACK = false
					return nil, status.Error(codes.Unavailable, "simulated Reviewer close ACK loss")
				}
			}
		}
		return response, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for Reviewer Runtime composition: %v", err)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(interceptor))
	RegisterBridgeAPI(server, store)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial Reviewer Runtime composition Bridge: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := bridgev1.NewAgentRuntimeBridgeServiceClient(connection)
	tempDir := t.TempDir()
	inputPath := tempDir + "/reviewer-composition-input.json"
	beforeTrunkReleasePath := tempDir + "/before-trunk-release"
	trunkReleasePath := tempDir + "/release-trunk"
	input, err := json.Marshal(map[string]any{
		"bridgeAddress": listener.Addr().String(), "workspaceId": "default", "sessionId": sessionID,
		"sessionThreadId": parentID, "bindingId": bindingID, "bindingGeneration": 1, "targetPodUid": podUID,
		"parentBoundaryEventId":  parentBoundaryEventID,
		"beforeTrunkReleasePath": beforeTrunkReleasePath, "trunkReleasePath": trunkReleasePath,
	})
	if err != nil {
		t.Fatalf("encode Reviewer Runtime composition input: %v", err)
	}
	if err := os.WriteFile(inputPath, input, 0o600); err != nil {
		t.Fatalf("write Reviewer Runtime composition input: %v", err)
	}
	registry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{
		"daytona": &bridgeMemoryProjectionProvider{},
	})
	if err != nil {
		t.Fatalf("build Reviewer output-capture provider registry: %v", err)
	}
	captureRunner := &tetralsandbox.SandboxOutputCaptureJobRunner{
		Queue:     tetralqueue.NewServer(queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime)), nil),
		Store:     tetralsandbox.NewPostgreSQLSandboxOutputCaptureStore(dbconnect.NewClientForTesting(runtime)),
		Providers: registry,
		BlobStore: blob.NewFakeBlobStore(),
		Config: tetralsandbox.SandboxOutputCaptureRunnerConfig{
			WorkspaceID: "default", LeaseOwner: "reviewer-runtime-output-capture", MaxJobs: 1,
			LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second,
		},
	}
	type captureRunResult struct {
		jobs int
		err  error
	}
	captureCtx, stopCapture := context.WithCancel(context.Background())
	captureFinished := make(chan captureRunResult, 1)
	go func() {
		jobs := 0
		for {
			if captureCtx.Err() != nil {
				captureFinished <- captureRunResult{jobs: jobs}
				return
			}
			active, captureErr := captureRunner.RunOnceWithActivity(captureCtx)
			if captureErr != nil {
				if captureCtx.Err() != nil || errors.Is(captureErr, context.Canceled) {
					captureFinished <- captureRunResult{jobs: jobs}
				} else {
					captureFinished <- captureRunResult{jobs: jobs, err: captureErr}
				}
				return
			}
			if active {
				jobs++
				continue
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	var runtimeOutput bytes.Buffer
	command := exec.Command("bun", "packages/runtime-pod/test/fixtures/reviewer-admission-composition.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	command.Dir = "../agent-runtime"
	command.Stdout = &runtimeOutput
	command.Stderr = &runtimeOutput
	if err := command.Start(); err != nil {
		t.Fatalf("start Reviewer Runtime composition: %v", err)
	}
	waitForCompositionFile(t, beforeTrunkReleasePath, "running Reviewer before target interrupt", &runtimeOutput)
	eventService := sessionevent.NewService(sessionevent.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime)))
	interruptBirth, err := eventService.AppendClientEvents(context.Background(), "default", sessionID, "reviewer-target-interrupt", sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{
		Type: sessionevent.EventTypeUserInterrupt, SessionThreadID: parentID,
	}}})
	if err != nil || len(interruptBirth.Data) != 1 {
		t.Fatalf("birth target interrupt while Reviewer runs = %#v/%v", interruptBirth, err)
	}
	var interruptInputID string
	if err := admin.QueryRowContext(context.Background(), `SELECT runtime_input_id FROM session_runtime_inbox
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND input_kind='interrupt_control'
		  AND event_ids_json::jsonb ? $3`, sessionID, parentID, interruptBirth.Data[0].ID).Scan(&interruptInputID); err != nil {
		t.Fatalf("read Reviewer target interrupt custody: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	interruptLease := mustLeaseBridgeQueueJob(t, queueStore, queue.LeaseRequest{
		WorkspaceID: "default", Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "reviewer-target-interrupt",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC(),
	})
	interruptJob := RuntimeJob{
		JobID: interruptLease.ID, LeaseToken: interruptLease.LeaseToken, Kind: interruptLease.Kind,
		PartitionKey: interruptLease.PartitionKey, DedupeKey: interruptLease.DedupeKey,
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: parentID,
		RuntimeInputID: interruptInputID, InputKind: "interrupt_control",
		EventIDs: []string{interruptBirth.Data[0].ID}, SequenceFrom: interruptBirth.Data[0].Sequence,
		SequenceTo: interruptBirth.Data[0].Sequence, PayloadJSON: string(interruptLease.PayloadJSON),
		AttemptCount: int32(interruptLease.AttemptCount), MaxAttempts: int32(interruptLease.MaxAttempts),
	}
	if _, err := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090).PrepareRuntimeCommand(context.Background(), interruptJob); err != nil {
		t.Fatalf("prepare Reviewer target interrupt delivery: %v", err)
	}
	parentEnd, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: parentScope, RuntimeWriteId: "rwrite_reviewer_composition_parent_end", ModelRequestId: "mreq_reviewer_composition_parent",
		FinishReason: "cancelled", UsageJson: `{}`, IsError: true, ErrorKind: "runtime_interrupted",
		ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "interrupted"},
		InterruptSettlement: &bridgev1.RequestEndInterruptSettlement{
			RuntimeInputId: interruptInputID, InterruptLeaseRef: bridgeInterruptLeaseRef(interruptLease),
		},
	})
	if err != nil || parentEnd.GetCommitted() == nil {
		t.Fatalf("terminalize reviewed target while Reviewer runs = %#v/%v; want committed closeout", parentEnd, err)
	}
	var parentEnds int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		  AND type='span.model_request_end' AND model_request_id='mreq_reviewer_composition_parent'`, sessionID, parentID).Scan(&parentEnds); err != nil || parentEnds != 1 {
		t.Fatalf("reviewed target Request Ends = %d/%v; want one durable end", parentEnds, err)
	}
	if err := os.WriteFile(trunkReleasePath, []byte("release"), 0o600); err != nil {
		t.Fatalf("release running Reviewer: %v", err)
	}
	err = command.Wait()
	output := runtimeOutput.Bytes()
	stopCapture()
	captureResult := <-captureFinished
	if captureResult.err != nil {
		t.Fatalf("run Reviewer output-capture owner: %v", captureResult.err)
	}
	if err != nil {
		t.Fatalf("run Reviewer Runtime composition: %v\n%s", err, output)
	}
	if captureResult.jobs < 3 {
		t.Fatalf("Reviewer output-capture jobs = %d; want completed decision, failure, and trunk closeout captures", captureResult.jobs)
	}
	var composed reviewerRuntimeCompositionOutput
	if err := json.Unmarshal(output, &composed); err != nil {
		t.Fatalf("decode Reviewer Runtime composition: %v\n%s", err, output)
	}
	if composed.TrunkResult.Type != "decision" || composed.TrunkResult.Outcome != "allow" ||
		composed.Decision.Type != "decision" || composed.Decision.Outcome != "allow" ||
		composed.Failure.Type != "failed" || !composed.CancellationSettled ||
		composed.ProviderRequests != 4 || !composed.ManagerDisposed ||
		len(composed.HotStateBeforeDispose.EphemeralReviewIDs) != 0 {
		t.Fatalf("Reviewer Runtime composition = %+v; want decision/failure/interrupt exits and released manager state; output=%s", composed, output)
	}
	targetApplication, err := client.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: parentScope, RuntimeWriteId: "rwrite_reviewer_composition_target_tool", ModelRequestId: "mreq_reviewer_composition_parent",
		ToolDeclaration: bridgeToolDeclarationForTest("tool_call_reviewer_trunk", "Write", `{"path":"src/a.ts","content":"ok"}`, "allow", "sandbox_execute"),
	})
	if err != nil || targetApplication.GetStale() == nil {
		t.Fatalf("apply late Reviewer allow to terminal target = %#v/%v; want typed stale", targetApplication, err)
	}
	ordinaryMember, ordinaryMemberErr := client.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: parentScope, RuntimeWriteId: "rwrite_reviewer_composition_late_message", ModelRequestId: "mreq_reviewer_composition_parent",
		EventType: "agent.message", PayloadJson: `{"type":"agent.message","content":[{"type":"text","text":"late"}]}`,
		AssistantContextDelta: bridgeTextContextDeltaForTest("late"),
	})
	if ordinaryMember != nil || status.Code(ordinaryMemberErr) != codes.FailedPrecondition {
		t.Fatalf("write ordinary member to terminal target = %#v/%v; want FailedPrecondition", ordinaryMember, ordinaryMemberErr)
	}
	if updated, err := queueStore.Ack(context.Background(), queue.AckRequest{
		WorkspaceID: "default", JobID: interruptLease.ID, LeaseToken: interruptLease.LeaseToken, Now: time.Now().UTC(),
	}); err != nil || !updated {
		t.Fatalf("settle Reviewer target interrupt Queue lease = %t/%v", updated, err)
	}
	if len(composed.Creations) != 4 || !composed.Creations[0].IsTrunk {
		t.Fatalf("Reviewer selection sequence = %+v; want one trunk followed by three sidecars", composed.Creations)
	}
	seenReviews := make(map[string]bool, len(composed.Creations))
	for index, creation := range composed.Creations {
		if index > 0 && creation.IsTrunk {
			t.Fatalf("Reviewer creation %d unexpectedly reused trunk: %+v", index, creation)
		}
		if creation.ReviewID == "" || seenReviews[creation.ReviewID] {
			t.Fatalf("Reviewer creation identity is empty or reused: %+v", composed.Creations)
		}
		seenReviews[creation.ReviewID] = true
	}
	for index, execution := range composed.HotStateBeforeDispose.Executions {
		if execution != nil {
			t.Fatalf("Reviewer Runtime execution %d remained hot before manager disposal: %#v", index, execution)
		}
	}
	closeFirstThread := seedQuiescentReviewerSidecar(t, client, parentScope, "review_close_first")
	closeFirst := &bridgev1.CloseApprovalReviewerRequest{
		Scope: parentScope, ReviewerThreadId: closeFirstThread, ReviewId: "review_close_first",
		SettlementKind:    bridgev1.ApprovalReviewerCloseSettlementKind_APPROVAL_REVIEWER_CLOSE_SETTLEMENT_KIND_DECISION,
		SettlementEventId: "evt_reviewer_close_first_missing",
	}
	if response, closeErr := client.CloseApprovalReviewer(context.Background(), closeFirst); status.Code(closeErr) != codes.FailedPrecondition || response != nil {
		t.Fatalf("close before Runtime settlement = %#v/%v; want failed precondition", response, closeErr)
	}

	var decisions, failures, reviewerEnds, closed, open, closeOperations, unsettledReviewerInputs, targetToolFacts int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='approval_review.decision'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='approval_review.failure'),
		(SELECT count(*) FROM request_usage_details WHERE workspace_id='default' AND session_id=$1 AND request_kind='approval_reviewer'),
		(SELECT count(*) FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND role='approval_reviewer' AND status='closed_for_runtime'),
		(SELECT count(*) FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND role='approval_reviewer' AND status<>'closed_for_runtime'),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND operation='close_approval_reviewer'),
		(SELECT count(*) FROM session_runtime_inbox inbox JOIN session_threads thread
		 ON thread.workspace_id=inbox.workspace_id AND thread.session_id=inbox.session_id AND thread.id=inbox.session_thread_id
		 WHERE inbox.workspace_id='default' AND inbox.session_id=$1 AND thread.role='approval_reviewer'
		   AND inbox.status IN ('queued','delivering','accepted')),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		 AND type IN ('agent.tool_use','agent.tool_result'))`, sessionID, parentID).
		Scan(&decisions, &failures, &reviewerEnds, &closed, &open, &closeOperations, &unsettledReviewerInputs, &targetToolFacts); err != nil {
		t.Fatalf("read Reviewer Runtime durable census: %v", err)
	}
	if decisions != 2 || failures != 1 || reviewerEnds != 4 || closed != 3 || open != 2 || closeOperations != 3 || unsettledReviewerInputs != 0 || targetToolFacts != 0 {
		t.Fatalf("Reviewer durable decision/failure/requests/closed/open/closes/unsettled/target-tools = %d/%d/%d/%d/%d/%d/%d/%d; want 2/1/4/3/2/3/0/0",
			decisions, failures, reviewerEnds, closed, open, closeOperations, unsettledReviewerInputs, targetToolFacts)
	}

	callsMu.Lock()
	defer callsMu.Unlock()
	if admissionCalls != 6 {
		t.Fatalf("Reviewer admission calls = %d; want five admissions plus exact lost-ACK replay", admissionCalls)
	}
	var decisionCloseRequests []*bridgev1.CloseApprovalReviewerRequest
	for _, request := range closeRequests {
		if request.GetSettlementKind() == bridgev1.ApprovalReviewerCloseSettlementKind_APPROVAL_REVIEWER_CLOSE_SETTLEMENT_KIND_DECISION {
			decisionCloseRequests = append(decisionCloseRequests, request)
		}
	}
	if len(closeRequests) != 5 || len(decisionCloseRequests) != 3 || !proto.Equal(decisionCloseRequests[0], decisionCloseRequests[1]) {
		t.Fatalf("Reviewer close calls/all decisions = %d/%d; want negative close, exact lost-ACK replay, failure, and interrupt: %+v",
			len(closeRequests), len(decisionCloseRequests), closeRequests)
	}
	for _, request := range closeRequests[:4] {
		var eventType, eventThread string
		if err := admin.QueryRowContext(context.Background(), `SELECT type,session_thread_id FROM session_events
			WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, request.GetSettlementEventId()).
			Scan(&eventType, &eventThread); err != nil {
			t.Fatalf("read Runtime-produced Reviewer settlement %q: %v", request.GetSettlementEventId(), err)
		}
		if eventThread != request.GetReviewerThreadId() {
			t.Fatalf("Reviewer close settlement %q belongs to %q, want %q", request.GetSettlementEventId(), eventThread, request.GetReviewerThreadId())
		}
		if eventType != reviewerSettlementEventType(request.GetSettlementKind()) {
			t.Fatalf("Reviewer close settlement %q type = %q", request.GetSettlementEventId(), eventType)
		}
	}
}

func seedQuiescentReviewerSidecar(t *testing.T, client bridgev1.AgentRuntimeBridgeServiceClient, parentScope *bridgev1.RuntimeScope, reviewID string) string {
	t.Helper()
	ensured, err := client.EnsureApprovalReviewerSidecar(context.Background(), &bridgev1.EnsureApprovalReviewerSidecarRequest{Scope: parentScope, ReviewId: reviewID})
	if err != nil || ensured.GetCommitted().GetReviewerThreadId() == "" {
		t.Fatalf("ensure Reviewer sidecar %s = %#v/%v", reviewID, ensured, err)
	}
	threadID := ensured.GetCommitted().GetReviewerThreadId()
	admitted, err := client.AdmitApprovalReviewInput(context.Background(), &bridgev1.AdmitApprovalReviewInputRequest{
		Scope: parentScope, ReviewerThreadId: threadID, ReviewId: reviewID,
	})
	if err != nil || admitted.GetCommitted().GetRuntimeInputId() == "" {
		t.Fatalf("admit Reviewer sidecar %s = %#v/%v", reviewID, admitted, err)
	}
	childScope := scopeForThread(parentScope, threadID)
	committed, err := client.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope: childScope, RuntimeInputId: admitted.GetCommitted().GetRuntimeInputId(), ApprovalReviewText: []string{"review exact durable authority"},
	})
	if err != nil || committed.GetCommitted().GetContext() == nil {
		t.Fatalf("commit Reviewer input %s = %#v/%v", reviewID, committed, err)
	}
	return threadID
}

func reviewerSettlementEventType(kind bridgev1.ApprovalReviewerCloseSettlementKind) string {
	switch kind {
	case bridgev1.ApprovalReviewerCloseSettlementKind_APPROVAL_REVIEWER_CLOSE_SETTLEMENT_KIND_DECISION:
		return "approval_review.decision"
	case bridgev1.ApprovalReviewerCloseSettlementKind_APPROVAL_REVIEWER_CLOSE_SETTLEMENT_KIND_FAILURE:
		return "approval_review.failure"
	case bridgev1.ApprovalReviewerCloseSettlementKind_APPROVAL_REVIEWER_CLOSE_SETTLEMENT_KIND_INTERRUPTED_REQUEST:
		return "span.model_request_end"
	default:
		return ""
	}
}
