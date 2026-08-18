package agentruntimebridge

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

type reviewerRuntimeCompositionOutput struct {
	LostAck struct {
		Type string `json:"type"`
	} `json:"lostAck"`
	TrunkResult struct {
		Type    string `json:"type"`
		Outcome string `json:"outcome"`
	} `json:"trunkResult"`
	Sidecar struct {
		Type    string `json:"type"`
		Outcome string `json:"outcome"`
	} `json:"sidecar"`
	ProviderRequests        int `json:"providerRequests"`
	DurableDecisionReceipts int `json:"durableDecisionReceipts"`
	Creations               []struct {
		ReviewID string `json:"reviewId"`
		IsTrunk  bool   `json:"isTrunk"`
	} `json:"creations"`
	HotStateBeforeDispose struct {
		Executions         []any    `json:"executions"`
		EphemeralReviewIDs []string `json:"ephemeralReviewIds"`
	} `json:"hotStateBeforeDispose"`
	ManagerDisposed bool `json:"managerDisposed"`
}

func TestPostgreSQLReviewerSelectionAdmissionAndLostACKCrossRuntimeAndBridge(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_reviewer_runtime_composition"
		parentID  = "thr_reviewer_runtime_composition_parent"
		bindingID = "bind_reviewer_runtime_composition"
		podUID    = "pod_reviewer_runtime_composition"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)

	var callsMu sync.Mutex
	var calls []string
	lostAdmissionACK := true
	interceptor := func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		response, err := handler(ctx, request)
		callsMu.Lock()
		calls = append(calls, info.FullMethod)
		loseACK := info.FullMethod == bridgev1.AgentRuntimeBridgeService_AdmitApprovalReviewInput_FullMethodName && lostAdmissionACK && err == nil
		if loseACK {
			lostAdmissionACK = false
		}
		callsMu.Unlock()
		if loseACK {
			return nil, status.Error(codes.Unavailable, "simulated admission ACK loss")
		}
		return response, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for Reviewer composition Bridge: %v", err)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(interceptor))
	RegisterBridgeAPI(server, NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime)))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	inputPath := t.TempDir() + "/reviewer-composition-input.json"
	input, err := json.Marshal(map[string]any{
		"bridgeAddress": listener.Addr().String(), "workspaceId": "default", "sessionId": sessionID,
		"sessionThreadId": parentID, "bindingId": bindingID, "bindingGeneration": 1, "targetPodUid": podUID,
	})
	if err != nil {
		t.Fatalf("encode Reviewer Runtime composition input: %v", err)
	}
	if err := os.WriteFile(inputPath, input, 0o600); err != nil {
		t.Fatalf("write Reviewer Runtime composition input: %v", err)
	}
	command := exec.Command("bun", "packages/runtime-pod/test/fixtures/reviewer-admission-composition.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run Reviewer Runtime composition: %v\n%s", err, output)
	}
	var composed reviewerRuntimeCompositionOutput
	if err := json.Unmarshal(output, &composed); err != nil {
		t.Fatalf("decode Reviewer Runtime composition: %v\n%s", err, output)
	}
	if composed.LostAck.Type != "settlement_failed" || composed.TrunkResult.Type != "decision" || composed.TrunkResult.Outcome != "allow" ||
		composed.Sidecar.Type != "decision" || composed.Sidecar.Outcome != "allow" || composed.ProviderRequests != 2 || composed.DurableDecisionReceipts != 2 || !composed.ManagerDisposed ||
		len(composed.HotStateBeforeDispose.EphemeralReviewIDs) != 0 {
		t.Fatalf("Reviewer Runtime composition = %+v; want lost-ACK uncertainty, trunk+sidecar decisions, two providers, and released manager state", composed)
	}
	if len(composed.Creations) != 3 || !composed.Creations[0].IsTrunk || !composed.Creations[1].IsTrunk || composed.Creations[2].IsTrunk ||
		composed.Creations[0].ReviewID != composed.Creations[1].ReviewID || composed.Creations[1].ReviewID == composed.Creations[2].ReviewID {
		t.Fatalf("Reviewer selection sequence = %+v; want one replayed trunk identity followed by one distinct sidecar identity", composed.Creations)
	}
	for index, execution := range composed.HotStateBeforeDispose.Executions {
		if execution != nil {
			t.Fatalf("Reviewer Runtime execution %d remained hot before manager disposal: %#v", index, execution)
		}
	}

	var reviewerInputs, acceptedInputs, reviewerThreads, trunks, sidecars int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND input_kind='approval_review'),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND input_kind='approval_review' AND status='accepted'),
		(SELECT count(*) FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND role='approval_reviewer'),
		(SELECT count(*) FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND role='approval_reviewer' AND is_trunk),
		(SELECT count(*) FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND role='approval_reviewer' AND NOT is_trunk)`, sessionID).
		Scan(&reviewerInputs, &acceptedInputs, &reviewerThreads, &trunks, &sidecars); err != nil {
		t.Fatalf("read composed Reviewer ownership: %v", err)
	}
	if reviewerInputs != 2 || acceptedInputs != 2 || reviewerThreads != 2 || trunks != 1 || sidecars != 1 {
		t.Fatalf("composed Reviewer inputs/accepted/threads/trunks/sidecars = %d/%d/%d/%d/%d; want 2/2/2/1/1",
			reviewerInputs, acceptedInputs, reviewerThreads, trunks, sidecars)
	}

	callsMu.Lock()
	defer callsMu.Unlock()
	var trunkEnsures, sidecarEnsures, admissions int
	for _, call := range calls {
		switch call {
		case bridgev1.AgentRuntimeBridgeService_EnsureApprovalReviewerTrunk_FullMethodName:
			trunkEnsures++
		case bridgev1.AgentRuntimeBridgeService_EnsureApprovalReviewerSidecar_FullMethodName:
			sidecarEnsures++
		case bridgev1.AgentRuntimeBridgeService_AdmitApprovalReviewInput_FullMethodName:
			admissions++
		}
	}
	if trunkEnsures != 2 || sidecarEnsures != 1 || admissions != 3 {
		t.Fatalf("Reviewer Ensure/Admission calls = trunk:%d sidecar:%d admission:%d; want 2 replayed trunk/1 sidecar/3 including lost-ACK replay: %v",
			trunkEnsures, sidecarEnsures, admissions, calls)
	}
}
