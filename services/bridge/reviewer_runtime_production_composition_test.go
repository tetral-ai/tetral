package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

type reviewerCloseCompositionCase struct {
	Request    map[string]any `json:"request"`
	ReviewID   string         `json:"reviewId"`
	IsTrunk    bool           `json:"isTrunk"`
	ThreadID   string         `json:"reviewerThreadId"`
	Settlement map[string]any `json:"settlement"`
}

func TestPostgreSQLReviewerCloseAuthorityCrossesRuntimeAdapterAndBridge(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_reviewer_close_composition"
		parentID  = "thr_reviewer_close_composition_parent"
		bindingID = "bind_reviewer_close_composition"
		podUID    = "pod_reviewer_close_composition"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("reviewer-close-composition-key")
	var callsMu sync.Mutex
	var closeRequests []*bridgev1.CloseApprovalReviewerRequest
	lostCloseACK := true
	interceptor := func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		response, err := handler(ctx, request)
		if info.FullMethod != bridgev1.AgentRuntimeBridgeService_CloseApprovalReviewer_FullMethodName {
			return response, err
		}
		closeRequest, ok := request.(*bridgev1.CloseApprovalReviewerRequest)
		if !ok {
			return response, err
		}
		callsMu.Lock()
		closeRequests = append(closeRequests, closeRequest)
		loseACK := lostCloseACK && closeRequest.GetReviewId() == "review_close_decision" && err == nil
		if loseACK {
			lostCloseACK = false
		}
		callsMu.Unlock()
		if loseACK {
			return nil, status.Error(codes.Unavailable, "simulated reviewer close ACK loss")
		}
		return response, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for Reviewer close composition: %v", err)
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
		t.Fatalf("dial Reviewer close composition Bridge: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := bridgev1.NewAgentRuntimeBridgeServiceClient(connection)
	parentScope := bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID)
	if trunk, ensureErr := client.EnsureApprovalReviewerTrunk(context.Background(), &bridgev1.EnsureApprovalReviewerTrunkRequest{
		Scope: parentScope, EnsureOperationId: "ensure_reviewer_close_composition_trunk",
	}); ensureErr != nil || trunk.GetCommitted().GetReviewerThreadId() == "" {
		t.Fatalf("ensure Reviewer close composition trunk = %#v/%v", trunk, ensureErr)
	}

	decisionThread := seedQuiescentReviewerSidecar(t, client, parentScope, "review_close_decision")
	seedReviewerOutcomeEvent(t, admin, sessionID, decisionThread, "evt_reviewer_close_decision", "approval_review.decision", "review_close_decision",
		`{"type":"approval_review.decision","review_id":"review_close_decision","outcome":"allow"}`)
	failureThread := seedQuiescentReviewerSidecar(t, client, parentScope, "review_close_failure")
	seedReviewerOutcomeEvent(t, admin, sessionID, failureThread, "evt_reviewer_close_failure", "approval_review.failure", "review_close_failure",
		`{"type":"approval_review.failure","review_id":"review_close_failure","failure_kind":"runtime_failure"}`)
	interruptThread := seedQuiescentReviewerSidecar(t, client, parentScope, "review_close_interrupt")
	seedInterruptedReviewerRequest(t, admin, sessionID, interruptThread)
	closeFirstThread := seedQuiescentReviewerSidecar(t, client, parentScope, "review_close_first")
	closeFirstRequest := &bridgev1.CloseApprovalReviewerRequest{
		Scope: parentScope, ReviewerThreadId: closeFirstThread, ReviewId: "review_close_first",
		SettlementKind:    bridgev1.ApprovalReviewerCloseSettlementKind_APPROVAL_REVIEWER_CLOSE_SETTLEMENT_KIND_DECISION,
		SettlementEventId: "evt_reviewer_close_first",
	}
	if response, closeErr := client.CloseApprovalReviewer(context.Background(), closeFirstRequest); status.Code(closeErr) != codes.FailedPrecondition || response != nil {
		t.Fatalf("close before settlement = %#v/%v; want failed precondition", response, closeErr)
	}
	seedReviewerOutcomeEvent(t, admin, sessionID, closeFirstThread, "evt_reviewer_close_first", "approval_review.decision", "review_close_first",
		`{"type":"approval_review.decision","review_id":"review_close_first","outcome":"deny"}`)

	conflictThread := seedQuiescentReviewerSidecar(t, client, parentScope, "review_close_conflict")
	seedReviewerOutcomeEvent(t, admin, sessionID, conflictThread, "evt_reviewer_close_conflict", "approval_review.decision", "another_review",
		`{"type":"approval_review.decision","review_id":"another_review","outcome":"allow"}`)
	if response, closeErr := client.CloseApprovalReviewer(context.Background(), &bridgev1.CloseApprovalReviewerRequest{
		Scope: parentScope, ReviewerThreadId: conflictThread, ReviewId: "review_close_conflict",
		SettlementKind:    bridgev1.ApprovalReviewerCloseSettlementKind_APPROVAL_REVIEWER_CLOSE_SETTLEMENT_KIND_DECISION,
		SettlementEventId: "evt_reviewer_close_conflict",
	}); status.Code(closeErr) != codes.FailedPrecondition || response != nil {
		t.Fatalf("cross-review close = %#v/%v; want failed precondition", response, closeErr)
	}

	closes := []reviewerCloseCompositionCase{
		reviewerCloseCase(parentScope, decisionThread, "review_close_decision", "decision", "evt_reviewer_close_decision"),
		reviewerCloseCase(parentScope, decisionThread, "review_close_decision", "decision", "evt_reviewer_close_decision"),
		reviewerCloseCase(parentScope, failureThread, "review_close_failure", "failure", "evt_reviewer_close_failure"),
		reviewerCloseCase(parentScope, interruptThread, "review_close_interrupt", "interrupted_request", "evt_reviewer_close_interrupt_end"),
		reviewerCloseCase(parentScope, closeFirstThread, "review_close_first", "decision", "evt_reviewer_close_first"),
	}
	inputPath := t.TempDir() + "/reviewer-close-input.json"
	input, err := json.Marshal(map[string]any{"bridgeAddress": listener.Addr().String(), "closes": closes})
	if err != nil {
		t.Fatalf("encode Reviewer close composition input: %v", err)
	}
	if err := os.WriteFile(inputPath, input, 0o600); err != nil {
		t.Fatalf("write Reviewer close composition input: %v", err)
	}
	command := exec.Command("bun", "packages/runtime-pod/test/fixtures/reviewer-admission-composition.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run Reviewer close composition: %v\n%s", err, output)
	}
	var composed struct {
		Results []struct {
			OK bool `json:"ok"`
		} `json:"results"`
	}
	if err := json.Unmarshal(output, &composed); err != nil || len(composed.Results) != len(closes) {
		t.Fatalf("decode Reviewer close composition: %v output=%s", err, output)
	}
	for index, result := range composed.Results {
		if !result.OK {
			t.Fatalf("Reviewer close result %d = %#v; want committed/duplicate", index, result)
		}
	}

	callsMu.Lock()
	defer callsMu.Unlock()
	var decisionCalls int
	for _, request := range closeRequests {
		if request.GetReviewId() == "review_close_decision" {
			decisionCalls++
		}
	}
	if decisionCalls != 3 || len(closeRequests) != len(closes)+3 {
		t.Fatalf("Reviewer close calls decision/all = %d/%d; want 3/%d including lost ACK and two rejected preflights", decisionCalls, len(closeRequests), len(closes)+3)
	}
	if closeRequests[2].GetReviewId() != "review_close_decision" || !proto.Equal(closeRequests[2], closeRequests[3]) {
		t.Fatalf("lost-ACK close did not replay exact decision request: %#v / %#v", closeRequests[2], closeRequests[3])
	}

	var closed, open int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		count(*) FILTER (WHERE status='closed_for_runtime'),
		count(*) FILTER (WHERE status<>'closed_for_runtime')
		FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND role='approval_reviewer'`, sessionID).
		Scan(&closed, &open); err != nil {
		t.Fatalf("read Reviewer close status census: %v", err)
	}
	if closed != 4 || open != 2 {
		t.Fatalf("Reviewer close status closed/open = %d/%d; want four closed, the trunk live, and the conflict untouched", closed, open)
	}
}

func seedReviewerOutcomeEvent(t *testing.T, db *sql.DB, sessionID, threadID, eventID, eventType, reviewID, payload string) {
	t.Helper()
	seedActorSourceEvent(t, db, sessionID, threadID, eventID, eventType, payload)
	suffix := "_decision"
	if eventType == "approval_review.failure" {
		suffix = "_failure"
	}
	if _, err := db.ExecContext(context.Background(), `UPDATE session_events SET runtime_write_id=$2
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$3`, sessionID, "rwrite_"+reviewID+suffix, eventID); err != nil {
		t.Fatalf("bind Reviewer outcome operation identity: %v", err)
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

func seedInterruptedReviewerRequest(t *testing.T, db *sql.DB, sessionID, threadID string) {
	t.Helper()
	const modelRequestID = "mreq_reviewer_close_interrupt"
	seedActorSourceEvent(t, db, sessionID, threadID, "evt_reviewer_close_interrupt_start", "span.model_request_start",
		`{"type":"span.model_request_start","model_request_id":"`+modelRequestID+`","request_kind":"approval_reviewer"}`)
	seedActorSourceEvent(t, db, sessionID, threadID, "evt_reviewer_close_interrupt_end", "span.model_request_end",
		`{"type":"span.model_request_end","model_request_id":"`+modelRequestID+`","error_kind":"runtime_interrupted","finish_reason":"cancelled"}`)
	if _, err := db.ExecContext(context.Background(), `UPDATE session_events SET model_request_id=$3
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		AND event_id IN ('evt_reviewer_close_interrupt_start','evt_reviewer_close_interrupt_end')`, sessionID, threadID, modelRequestID); err != nil {
		t.Fatalf("bind interrupted Reviewer request events: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `INSERT INTO request_usage_details (
		workspace_id,session_id,session_thread_id,model_request_id,runtime_write_id,request_kind,
		input_total_tokens,input_uncached_tokens,output_total_tokens,created_at)
		VALUES ('default',$1,$2,$3,'rwrite_reviewer_close_interrupt','approval_reviewer',0,0,0,now())`, sessionID, threadID, modelRequestID); err != nil {
		t.Fatalf("seed interrupted Reviewer request usage: %v", err)
	}
}

func reviewerCloseCase(scope *bridgev1.RuntimeScope, threadID, reviewID, settlementType, eventID string) reviewerCloseCompositionCase {
	return reviewerCloseCompositionCase{
		Request: map[string]any{
			"workspaceId": scope.GetWorkspaceId(), "sessionId": scope.GetSessionId(), "sessionThreadId": scope.GetSessionThreadId(),
			"bindingId": scope.GetBinding().GetBindingId(), "bindingGeneration": scope.GetBinding().GetBindingGeneration(),
			"targetPodUid": scope.GetBinding().GetTargetPodUid(), "runtimeBindingToken": "unused-reviewer-close-token",
		},
		ReviewID: reviewID, IsTrunk: false, ThreadID: threadID,
		Settlement: map[string]any{"type": settlementType, "eventId": eventID},
	}
}
