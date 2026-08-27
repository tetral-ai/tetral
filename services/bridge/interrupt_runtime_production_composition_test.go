package agentruntimebridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

type interruptRuntimeCompositionProcess struct {
	command *exec.Cmd
	output  bytes.Buffer
	port    int
}

type interruptRuntimeCompositionOutput struct {
	InterruptResult             json.RawMessage   `json:"interruptResult"`
	InterruptResults            []json.RawMessage `json:"interruptResults"`
	ThreadSnapshot              json.RawMessage   `json:"threadSnapshot"`
	FinishIdleInvocations       int               `json:"finishIdleInvocations"`
	ProviderInvocations         int               `json:"providerInvocations"`
	ProviderContexts            []json.RawMessage `json:"providerContexts"`
	DurableOperationCompletions int               `json:"durableOperationCompletions"`
}

type interruptRuntimeCompositionOptions struct {
	failFirstFinishIdle bool
	fastThreadText      string
}

type interruptFollowerRuntimeServer struct {
	agentruntimev1.UnimplementedAgentRuntimePodServiceServer
	calls atomic.Int32
}

type committingInterruptRuntimeSender struct {
	*recordingRuntimeCommandSender
	bridge *PostgreSQLBridgeAPIStore
}

var errSyntheticInterruptResponseLoss = errors.New("synthetic interrupt response loss")

type interruptResponseLossSender struct {
	RuntimeCommandSender
	calls atomic.Int32
}

type interruptRequestCaptureSender struct {
	RuntimeCommandSender
	target  RuntimePodTarget
	request *agentruntimev1.InterruptRequest
}

type blockingRecoveryCommandSender struct {
	RuntimeCommandSender
	recovery   RuntimeRecoveryCommandSender
	planned    chan *agentruntimev1.RecoverThreadRequest
	release    chan struct{}
	targetPort atomic.Int32
}

func (s *blockingRecoveryCommandSender) RecoverThread(
	ctx context.Context,
	target RuntimePodTarget,
	request *agentruntimev1.RecoverThreadRequest,
) (*agentruntimev1.RecoverThreadResponse, error) {
	select {
	case s.planned <- request:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	target.Port = int(s.targetPort.Load())
	return s.recovery.RecoverThread(ctx, target, request)
}

func (s *interruptResponseLossSender) Interrupt(
	ctx context.Context,
	target RuntimePodTarget,
	request *agentruntimev1.InterruptRequest,
) (*agentruntimev1.InterruptResponse, error) {
	s.calls.Add(1)
	response, err := s.RuntimeCommandSender.Interrupt(ctx, target, request)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, errors.New("interrupt response is nil")
	}
	return nil, errSyntheticInterruptResponseLoss
}

func (s *interruptRequestCaptureSender) Interrupt(
	ctx context.Context,
	target RuntimePodTarget,
	request *agentruntimev1.InterruptRequest,
) (*agentruntimev1.InterruptResponse, error) {
	s.target = target
	s.request = request
	return s.RuntimeCommandSender.Interrupt(ctx, target, request)
}

func (s *interruptResponseLossSender) RecoverThread(
	ctx context.Context,
	target RuntimePodTarget,
	request *agentruntimev1.RecoverThreadRequest,
) (*agentruntimev1.RecoverThreadResponse, error) {
	recovery, ok := s.RuntimeCommandSender.(RuntimeRecoveryCommandSender)
	if !ok {
		return nil, errors.New("runtime recovery sender is unavailable")
	}
	return recovery.RecoverThread(ctx, target, request)
}

func (s *committingInterruptRuntimeSender) Interrupt(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.InterruptRequest) (*agentruntimev1.InterruptResponse, error) {
	s.targets = append(s.targets, target)
	s.requests = append(s.requests, request)
	lease := request.GetInterruptLeaseRef()
	committed, err := s.bridge.CommitInputs(ctx, &bridgev1.CommitInputsRequest{
		Scope: bridgeAPIScope(
			request.GetSessionId(), request.GetSessionThreadId(), request.GetBindingId(),
			request.GetBindingGeneration(), request.GetTargetPodUid(),
		),
		RuntimeInputId: request.GetRuntimeInputId(),
		InterruptLeaseRef: &bridgev1.InterruptLeaseRef{
			JobId: lease.GetJobId(), LeaseToken: lease.GetLeaseToken(),
			PartitionKey: lease.GetPartitionKey(), DedupeKey: lease.GetDedupeKey(),
		},
	})
	if err != nil {
		return nil, err
	}
	if committed.GetCommitted().GetInterrupt() == nil {
		return nil, status.Error(codes.FailedPrecondition, "fixture interrupt closeout was not committed")
	}
	return &agentruntimev1.InterruptResponse{Outcome: &agentruntimev1.InterruptResponse_Accepted{
		Accepted: &agentruntimev1.InterruptAccepted{},
	}}, nil
}

func (s *interruptFollowerRuntimeServer) AcceptInput(context.Context, *agentruntimev1.AcceptInputRequest) (*agentruntimev1.AcceptInputResponse, error) {
	switch s.calls.Add(1) {
	case 1:
		return nil, status.Error(codes.InvalidArgument, "fixture-controlled deterministic rejection")
	default:
		return &agentruntimev1.AcceptInputResponse{Outcome: &agentruntimev1.AcceptInputResponse_Rejected{Rejected: &agentruntimev1.AcceptInputRejected{
			Reason: agentruntimev1.AcceptInputFailure_ACCEPT_INPUT_FAILURE_IDENTITY_CONFLICT, Retryable: true,
		}}}, nil
	}
}

func TestPostgreSQLInterruptSettlesPreparedRejectionAndQueuedFollowers(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_interrupt_follower_production"
		threadID  = "thr_interrupt_follower_production"
		bindingID = "bind_interrupt_follower_production"
		podUID    = "pod_interrupt_follower_production"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)

	bridgeStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridgeStore.RuntimeBindingTokenHMACKey = []byte("interrupt-follower-production-key")
	bridgeListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for follower Bridge: %v", err)
	}
	bridgeServer := grpc.NewServer()
	RegisterBridgeAPI(bridgeServer, bridgeStore)
	go func() { _ = bridgeServer.Serve(bridgeListener) }()
	t.Cleanup(func() {
		bridgeServer.Stop()
		_ = bridgeListener.Close()
	})

	fixtureListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for follower Runtime boundary: %v", err)
	}
	fixtureServer := grpc.NewServer()
	followerRuntime := &interruptFollowerRuntimeServer{}
	agentruntimev1.RegisterAgentRuntimePodServiceServer(fixtureServer, followerRuntime)
	go func() { _ = fixtureServer.Serve(fixtureListener) }()
	t.Cleanup(func() {
		fixtureServer.Stop()
		_ = fixtureListener.Close()
	})
	fixturePort := fixtureListener.Addr().(*net.TCPAddr).Port

	client := dbconnect.NewClientForTesting(runtime)
	eventService := sessionevent.NewService(sessionevent.NewPostgreSQLStore(client))
	appendMessage := func(idempotencyKey, content string) string {
		t.Helper()
		result, appendErr := eventService.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, idempotencyKey,
			sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{
				Type:    sessionevent.EventTypeUserMessage,
				Content: []sessionevent.ContentBlock{{Type: sessionevent.ContentBlockTypeText, Text: content}},
			}}})
		if appendErr != nil || len(result.Data) != 1 {
			t.Fatalf("append follower message = %#v/%v", result, appendErr)
		}
		var runtimeInputID string
		if err := admin.QueryRowContext(context.Background(), `SELECT runtime_input_id
			FROM session_runtime_inbox
			WHERE workspace_id='default' AND session_id=$1 AND event_ids_json::jsonb ? $2`, sessionID, result.Data[0].ID).Scan(&runtimeInputID); err != nil {
			t.Fatalf("read follower Inbox identity: %v", err)
		}
		return runtimeInputID
	}

	queueStore := queue.NewPostgreSQLStore(client)
	newRunner := func(port int, owner string) *JobRunner {
		deliveryStore := NewPostgreSQLRuntimeDeliveryStore(client, port)
		deliveryStore.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
			return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
				Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: podUID, PodIP: "127.0.0.1",
			}})
		}}
		return &JobRunner{
			Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
			Deliverer: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: NewRuntimePodCommandClient(taskNotificationRuntimeTokenSource{})},
			Config:    JobRunnerConfig{LeaseOwner: owner, MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
		}
	}

	rejectionID := appendMessage("interrupt-follower-rejection", "reject before stop")
	if active, err := newRunner(fixturePort, "interrupt-follower-rejection").RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("prepare deterministic rejection follower = active:%t err:%v", active, err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs SET available_at=clock_timestamp()+interval '1 hour'
		WHERE workspace_id='default' AND dedupe_key=$1`, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, rejectionID)); err != nil {
		t.Fatalf("hold rejection follower behind interrupt: %v", err)
	}
	queuedID := appendMessage("interrupt-follower-queued", "queued before stop")
	var rejectionStatus, rejectionKind, queuedStatus, queuedKind string
	if err := admin.QueryRowContext(context.Background(), `SELECT
			(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
			(SELECT input_kind FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
			(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$2),
			(SELECT input_kind FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$2)`, rejectionID, queuedID).
		Scan(&rejectionStatus, &rejectionKind, &queuedStatus, &queuedKind); err != nil {
		t.Fatalf("read pre-interrupt follower custody: %v", err)
	}
	if rejectionStatus != "delivering" || rejectionKind != "rejection" || queuedStatus != "queued" || queuedKind != "messages" || followerRuntime.calls.Load() != 2 {
		t.Fatalf("pre-interrupt followers = rejection:%s/%s queued:%s/%s calls:%d", rejectionStatus, rejectionKind, queuedStatus, queuedKind, followerRuntime.calls.Load())
	}

	runtimeProcess, paths := startInterruptRuntimeComposition(t, t.TempDir(), bridgeListener.Addr().String(), sessionID, threadID, bindingID, 1, podUID)
	interruptBirth, err := eventService.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, "interrupt-follower-stop",
		sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{Type: sessionevent.EventTypeUserInterrupt}}})
	if err != nil || len(interruptBirth.Data) != 1 {
		t.Fatalf("birth follower interrupt = %#v/%v", interruptBirth, err)
	}
	var interruptID string
	if err := admin.QueryRowContext(context.Background(), `SELECT runtime_input_id FROM session_runtime_inbox
		WHERE workspace_id='default' AND session_id=$1 AND input_kind='interrupt_control'`, sessionID).Scan(&interruptID); err != nil {
		t.Fatalf("read follower interrupt identity: %v", err)
	}
	if active, err := newRunner(runtimeProcess.port, "interrupt-follower-closeout").RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("deliver follower interrupt through real Runtime = active:%t err:%v", active, err)
	}
	if err := os.WriteFile(paths.close, []byte("close"), 0o600); err != nil {
		t.Fatalf("release follower Runtime composition: %v", err)
	}
	composed := runtimeProcess.wait(t)
	if len(composed.InterruptResult) == 0 || composed.ProviderInvocations != 0 {
		t.Fatalf("follower interrupt Runtime composition = %+v", composed)
	}
	var interruptInbox, interruptQueue, rejectionInbox, rejectionQueue, queuedInbox, queuedQueue string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$2),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$3),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$4),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$5),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$6)`,
		interruptID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, interruptID),
		rejectionID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, rejectionID),
		queuedID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, queuedID)).
		Scan(&interruptInbox, &interruptQueue, &rejectionInbox, &rejectionQueue, &queuedInbox, &queuedQueue); err != nil {
		t.Fatalf("read terminal follower custody: %v", err)
	}
	if interruptInbox != "committed" || interruptQueue != queue.StatusAcknowledged ||
		rejectionInbox != "cancelled" || rejectionQueue != queue.StatusCancelled ||
		queuedInbox != "cancelled" || queuedQueue != queue.StatusCancelled {
		t.Fatalf("terminal follower custody = interrupt:%s/%s rejection:%s/%s queued:%s/%s",
			interruptInbox, interruptQueue, rejectionInbox, rejectionQueue, queuedInbox, queuedQueue)
	}
}

func TestPostgreSQLAcceptedMessageQueueResidueDoesNotFreezeInterrupt(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_interrupt_accepted_residue"
		threadID  = "thr_interrupt_accepted_residue"
		bindingID = "bind_interrupt_accepted_residue"
		podUID    = "pod_interrupt_accepted_residue"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)

	client := dbconnect.NewClientForTesting(runtime)
	eventService := sessionevent.NewService(sessionevent.NewPostgreSQLStore(client))
	messageBirth, err := eventService.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID,
		"interrupt-accepted-residue-message", sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{
			Type:    sessionevent.EventTypeUserMessage,
			Content: []sessionevent.ContentBlock{{Type: sessionevent.ContentBlockTypeText, Text: "accepted before stop"}},
		}}})
	if err != nil || len(messageBirth.Data) != 1 {
		t.Fatalf("birth accepted-residue message = %#v/%v", messageBirth, err)
	}
	var messageID string
	if err := admin.QueryRowContext(context.Background(), `SELECT runtime_input_id FROM session_runtime_inbox
		WHERE workspace_id='default' AND session_id=$1 AND event_ids_json::jsonb ? $2`, sessionID, messageBirth.Data[0].ID).
		Scan(&messageID); err != nil {
		t.Fatalf("read accepted-residue message identity: %v", err)
	}

	queueStore := queue.NewPostgreSQLStore(client)
	messageLease := mustLeaseBridgeQueueJob(t, queueStore, queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "accepted-residue-delivery",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC(),
	})
	messageJob, err := DecodeRuntimeJob(queueJobProto(messageLease))
	if err != nil {
		t.Fatalf("decode accepted-residue message lease: %v", err)
	}
	apiStore := NewPostgreSQLBridgeAPIStore(client)
	apiStore.RuntimeBindingTokenHMACKey = []byte("interrupt-accepted-residue-key")
	sender := &committingInterruptRuntimeSender{
		recordingRuntimeCommandSender: &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}},
		bridge:                        apiStore,
	}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(client, 9090)
	deliveryStore.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "engine", PodName: "runtime-accepted-residue", PodUID: podUID, PodIP: "127.0.0.1",
		}})
	}}
	direct := RuntimePodDirectDeliverer{Store: deliveryStore, Sender: sender}
	if result, err := direct.DeliverRuntimeJob(context.Background(), messageJob); err != nil || result.Status != RuntimeDeliveryAccepted {
		t.Fatalf("deliver accepted-residue message = %+v/%v", result, err)
	}
	var messageInbox, messageQueue string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2)`, messageID, messageLease.ID).
		Scan(&messageInbox, &messageQueue); err != nil {
		t.Fatalf("read accepted message before Queue reclaim: %v", err)
	}
	if messageInbox != "accepted" || messageQueue != queue.StatusLeased || len(sender.requests) != 1 {
		t.Fatalf("pre-reclaim accepted message = Inbox:%s Queue:%s Runtime calls:%d", messageInbox, messageQueue, len(sender.requests))
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs
		SET leased_until=clock_timestamp()-interval '1 second' WHERE workspace_id='default' AND id=$1`, messageLease.ID); err != nil {
		t.Fatalf("expire accepted message Queue ACK window: %v", err)
	}
	if reclaimed, err := queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{
		WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput, Limit: 1,
	}); err != nil || reclaimed != 1 {
		t.Fatalf("reclaim accepted message Queue residue = %d/%v; want 1/nil", reclaimed, err)
	}

	interruptBirth, err := eventService.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID,
		"interrupt-accepted-residue-stop", sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{Type: sessionevent.EventTypeUserInterrupt}}})
	if err != nil || len(interruptBirth.Data) != 1 {
		t.Fatalf("birth interrupt behind accepted Queue residue = %#v/%v", interruptBirth, err)
	}
	var interruptID string
	if err := admin.QueryRowContext(context.Background(), `SELECT runtime_input_id FROM session_runtime_inbox
		WHERE workspace_id='default' AND session_id=$1 AND event_ids_json::jsonb ? $2`, sessionID, interruptBirth.Data[0].ID).
		Scan(&interruptID); err != nil {
		t.Fatalf("read accepted-residue interrupt identity: %v", err)
	}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: direct,
		Config: JobRunnerConfig{LeaseOwner: "accepted-residue-closeout", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("deliver interrupt ahead of accepted Queue residue = active:%t err:%v", active, err)
	}
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("settle accepted Queue residue by accepted replay = active:%t err:%v", active, err)
	}
	var interruptInbox, interruptQueue string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$2),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$3),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$4)`,
		interruptID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, interruptID), messageID, messageLease.ID).
		Scan(&interruptInbox, &interruptQueue, &messageInbox, &messageQueue); err != nil {
		t.Fatalf("read accepted-residue terminal custody: %v", err)
	}
	if interruptInbox != "committed" || interruptQueue != queue.StatusAcknowledged ||
		messageInbox != "accepted" || messageQueue != queue.StatusAcknowledged || len(sender.requests) != 2 {
		t.Fatalf("accepted-residue terminal custody = interrupt:%s/%s message:%s/%s Runtime calls:%d",
			interruptInbox, interruptQueue, messageInbox, messageQueue, len(sender.requests))
	}
	if _, ok := sender.requests[0].(*agentruntimev1.AcceptInputRequest); !ok {
		t.Fatalf("first Runtime call = %T; want AcceptInput", sender.requests[0])
	}
	if _, ok := sender.requests[1].(*agentruntimev1.InterruptRequest); !ok {
		t.Fatalf("second Runtime call = %T; want Interrupt", sender.requests[1])
	}
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || active {
		t.Fatalf("accepted-residue partition after replay = active:%t err:%v; want drained", active, err)
	}
}

func TestPostgreSQLInterruptBarrierFollowsSessionQueueOrderAcrossThreads(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_interrupt_queue_order"
		mainID    = "thr_interrupt_queue_order_main"
		childID   = "thr_interrupt_queue_order_child"
		bindingID = "bind_interrupt_queue_order"
		podUID    = "pod_interrupt_queue_order"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	seedBridgeAPIEvent(t, admin, "default", sessionID, childID, "evt_interrupt_queue_order_child_history", 9,
		"session.status", `{"type":"session.status"}`)

	client := dbconnect.NewClientForTesting(runtime)
	eventService := sessionevent.NewService(sessionevent.NewPostgreSQLStore(client))
	childBirth, err := eventService.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID,
		"interrupt-queue-order-child", sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{
			Type: sessionevent.EventTypeUserInterrupt, SessionThreadID: childID,
		}}})
	if err != nil || len(childBirth.Data) != 1 {
		t.Fatalf("birth first explicit child interrupt = %#v/%v", childBirth, err)
	}
	mainBirth, err := eventService.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID,
		"interrupt-queue-order-main", sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{Type: sessionevent.EventTypeUserInterrupt}}})
	if err != nil || len(mainBirth.Data) != 1 {
		t.Fatalf("birth second bare main interrupt = %#v/%v", mainBirth, err)
	}
	var childInterruptID, mainInterruptID string
	var childSequence, mainSequence, childQueueSequence, mainQueueSequence int64
	if err := admin.QueryRowContext(context.Background(), `SELECT
		child.runtime_input_id, main.runtime_input_id, child.sequence_to, main.sequence_to,
		child_job.queue_partition_sequence, main_job.queue_partition_sequence
		FROM session_runtime_inbox child
		JOIN session_runtime_inbox main ON main.workspace_id=child.workspace_id AND main.session_id=child.session_id
		JOIN queue_jobs child_job ON child_job.workspace_id=child.workspace_id
		 AND child_job.dedupe_key='runtime_input:' || child.workspace_id || ':' || child.session_id || ':' || child.runtime_input_id
		JOIN queue_jobs main_job ON main_job.workspace_id=main.workspace_id
		 AND main_job.dedupe_key='runtime_input:' || main.workspace_id || ':' || main.session_id || ':' || main.runtime_input_id
		WHERE child.workspace_id='default' AND child.session_id=$1
		 AND child.event_ids_json::jsonb ? $2 AND main.event_ids_json::jsonb ? $3`,
		sessionID, childBirth.Data[0].ID, mainBirth.Data[0].ID).
		Scan(&childInterruptID, &mainInterruptID, &childSequence, &mainSequence, &childQueueSequence, &mainQueueSequence); err != nil {
		t.Fatalf("read cross-thread interrupt order: %v", err)
	}
	if childSequence <= mainSequence || childQueueSequence >= mainQueueSequence {
		t.Fatalf("interrupt order fixture = thread sequences child/main %d/%d, Queue sequences child/main %d/%d; want inverted",
			childSequence, mainSequence, childQueueSequence, mainQueueSequence)
	}

	apiStore := NewPostgreSQLBridgeAPIStore(client)
	apiStore.RuntimeBindingTokenHMACKey = []byte("interrupt-queue-order-key")
	sender := &committingInterruptRuntimeSender{
		recordingRuntimeCommandSender: &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}},
		bridge:                        apiStore,
	}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(client, 9090)
	deliveryStore.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "engine", PodName: "runtime-interrupt-queue-order", PodUID: podUID, PodIP: "127.0.0.1",
		}})
	}}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queue.NewPostgreSQLStore(client), nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: sender},
		Config:    JobRunnerConfig{LeaseOwner: "interrupt-queue-order", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("process first-born child interrupt = active:%t err:%v", active, err)
	}
	if len(sender.requests) != 1 {
		t.Fatalf("first Runtime interrupt = %#v; want child %s", sender.requests, childInterruptID)
	}
	firstInterrupt, ok := sender.requests[0].(*agentruntimev1.InterruptRequest)
	if !ok || firstInterrupt.GetRuntimeInputId() != childInterruptID {
		t.Fatalf("first Runtime interrupt = %#v; want child %s", sender.requests[0], childInterruptID)
	}
	var childInbox, childQueue, mainInbox, mainQueue string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$2),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$3),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$4)`,
		childInterruptID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, childInterruptID),
		mainInterruptID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, mainInterruptID)).
		Scan(&childInbox, &childQueue, &mainInbox, &mainQueue); err != nil {
		t.Fatalf("read custody after child interrupt: %v", err)
	}
	if childInbox != "committed" || childQueue != queue.StatusAcknowledged || mainInbox != "queued" || mainQueue != queue.StatusPending {
		t.Fatalf("custody after child interrupt = child:%s/%s main:%s/%s", childInbox, childQueue, mainInbox, mainQueue)
	}
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("process second-born main interrupt = active:%t err:%v", active, err)
	}
	if len(sender.requests) != 2 {
		t.Fatalf("second Runtime interrupt = %#v; want main %s", sender.requests, mainInterruptID)
	}
	secondInterrupt, ok := sender.requests[1].(*agentruntimev1.InterruptRequest)
	if !ok || secondInterrupt.GetRuntimeInputId() != mainInterruptID {
		t.Fatalf("second Runtime interrupt = %#v; want main %s", sender.requests[1], mainInterruptID)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$2),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$3),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$4)`,
		childInterruptID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, childInterruptID),
		mainInterruptID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, mainInterruptID)).
		Scan(&childInbox, &childQueue, &mainInbox, &mainQueue); err != nil {
		t.Fatalf("read terminal ordered interrupt custody: %v", err)
	}
	if childInbox != "committed" || childQueue != queue.StatusAcknowledged || mainInbox != "committed" || mainQueue != queue.StatusAcknowledged {
		t.Fatalf("terminal ordered interrupt custody = child:%s/%s main:%s/%s", childInbox, childQueue, mainInbox, mainQueue)
	}
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || active {
		t.Fatalf("ordered interrupt partition after closeout = active:%t err:%v; want drained", active, err)
	}
}

func TestPostgreSQLInterruptBlocksAtRuntimeUntilBridgeCloseoutCompletes(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID    = "sesn_interrupt_production_composition"
		threadID     = "thr_interrupt_production_composition"
		bindingID    = "bind_interrupt_production_composition"
		podUID       = "pod_interrupt_production_composition"
		messageID    = "rin_interrupt_production_message"
		messageEvent = "evt_interrupt_production_message"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, messageEvent, 1, "user.message", `{"content":[{"type":"text","text":"run the durable operation"}]}`)
	messageJob := RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: messageID, InputKind: "messages", EventIDs: []string{messageEvent}, SequenceFrom: 1, SequenceTo: 1,
		PayloadJSON: `{"workspace_id":"default","session_id":"` + sessionID + `","session_thread_id":"` + threadID + `","runtime_input_id":"` + messageID + `","event_ids":["` + messageEvent + `"],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, messageJob)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueueRuntimeCompositionJob(t, queueStore, sessionID, messageJob, 0)

	bridgeStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridgeStore.RuntimeBindingTokenHMACKey = []byte("interrupt-production-composition-key")
	bridgeListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for interrupt composition Bridge: %v", err)
	}
	bridgeServer := grpc.NewServer()
	RegisterBridgeAPI(bridgeServer, bridgeStore)
	go func() { _ = bridgeServer.Serve(bridgeListener) }()
	t.Cleanup(func() {
		bridgeServer.Stop()
		_ = bridgeListener.Close()
	})

	tempDir := t.TempDir()
	runtimeProcess, paths := startInterruptRuntimeComposition(t, tempDir, bridgeListener.Addr().String(), sessionID, threadID, bindingID, 1, podUID)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_bindings
		SET agent_runtime_pod_ip='127.0.0.1'
		WHERE workspace_id='default' AND session_id=$1 AND binding_id=$2`, sessionID, bindingID); err != nil {
		t.Fatalf("align Runtime binding: %v", err)
	}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), runtimeProcess.port)
	deliveryStore.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: podUID, PodIP: "127.0.0.1",
		}})
	}}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: NewRuntimePodCommandClient(taskNotificationRuntimeTokenSource{})},
		Config:    JobRunnerConfig{LeaseOwner: "interrupt-production-composition", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("deliver initial input through Runtime gRPC = active:%t err:%v", active, err)
	}
	if rawAccept, err := os.ReadFile(paths.acceptResult); err == nil {
		t.Logf("Runtime accept result: %s", rawAccept)
	}
	var diagnosticInbox string
	var diagnosticMessages, diagnosticStarts, diagnosticToolUses int
	diagnosticDeadline := time.Now().Add(10 * time.Second)
	for {
		if err := admin.QueryRowContext(context.Background(), `SELECT
			(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
			(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$2),
			(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$2 AND type='span.model_request_start'),
			(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$2 AND type='agent.tool_use')`,
			messageID, sessionID).Scan(&diagnosticInbox, &diagnosticMessages, &diagnosticStarts, &diagnosticToolUses); err != nil {
			t.Fatalf("read initial Runtime composition diagnostics: %v", err)
		}
		if diagnosticStarts == 1 && diagnosticToolUses == 1 {
			break
		}
		if time.Now().After(diagnosticDeadline) {
			t.Fatalf("Runtime did not persist the open Request and pending Tool: Inbox=%s Messages=%d starts=%d toolUses=%d: %s",
				diagnosticInbox, diagnosticMessages, diagnosticStarts, diagnosticToolUses, runtimeProcess.output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Logf("initial Runtime composition Inbox=%s Messages=%d starts=%d toolUses=%d", diagnosticInbox, diagnosticMessages, diagnosticStarts, diagnosticToolUses)
	waitForCompositionFile(t, paths.toolStarted, "durable Runtime operation", &runtimeProcess.output)

	// Cleanup may observe a durable idle projection while the pod still owns a
	// hot run slot. The Runtime is the final busy authority in that lag window.
	cleanupID := "cleanup_interrupt_production_hot"
	idleEventID := "evt_interrupt_production_lagging_idle"
	idleSequence := nextBridgeAPIEventSequenceForTest(t, admin, sessionID, threadID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, idleEventID, idleSequence, "session.status_idle", `{"type":"session.status_idle"}`)
	seedBridgeAPIStreamChange(t, admin, "default", sessionID, threadID, idleEventID, 1, "internal", false)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads SET status='idle', updated_at=clock_timestamp()
		WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, threadID); err != nil {
		t.Fatalf("seed lagging idle Thread projection: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_status
		SET status='idle', status_event_id=$2, cleanup_job_id=$3, cleanup_enqueued_at=clock_timestamp(), updated_at=clock_timestamp()
		WHERE workspace_id='default' AND session_id=$1`, sessionID, idleEventID, cleanupID); err != nil {
		t.Fatalf("seed lagging idle cleanup projection: %v", err)
	}
	cleanupJobID := queue.NewJobID()
	if _, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
		ID: cleanupJobID, WorkspaceID: workspace.DefaultID, Kind: queue.KindCleanupSession,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:      queue.FormatCleanupSessionDedupeKey(workspace.DefaultID, sessionID, cleanupID),
		PayloadVersion: 1,
		PayloadJSON:    []byte(`{"workspace_id":"default","session_id":"` + sessionID + `","cleanup_job_id":"` + cleanupID + `"}`),
		MaxAttempts:    queue.DefaultMaxAttempts, Now: time.Now().UTC().Add(-time.Second),
	}); err != nil {
		t.Fatalf("enqueue cleanup during hot Thread run: %v", err)
	}
	cleanupSender := &countingCleanupSender{RuntimeCommandSender: NewRuntimePodCommandClient(taskNotificationRuntimeTokenSource{})}
	cleanupRunner := &JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: cleanupSender},
		Config:    JobRunnerConfig{LeaseOwner: "interrupt-production-hot-cleanup", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	if active, err := cleanupRunner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("run cleanup against hot Thread = active:%t err:%v", active, err)
	}
	var cleanupQueueStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$1`, cleanupJobID).Scan(&cleanupQueueStatus); err != nil {
		t.Fatalf("read hot cleanup Queue state: %v", err)
	}
	if cleanupSender.calls.Load() != 1 || cleanupQueueStatus != queue.StatusAcknowledged {
		t.Fatalf("hot cleanup = Runtime calls:%d Queue:%s; want one busy check and acknowledged reschedule", cleanupSender.calls.Load(), cleanupQueueStatus)
	}
	assertCleanupMarkersRearmed(t, admin, sessionID, true)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads SET status='running', updated_at=clock_timestamp()
		WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, threadID); err != nil {
		t.Fatalf("restore running Thread projection: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_status
		SET status='running', status_event_id=NULL, cleanup_after=NULL, updated_at=clock_timestamp()
		WHERE workspace_id='default' AND session_id=$1`, sessionID); err != nil {
		t.Fatalf("restore running Session projection: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `DELETE FROM session_event_stream_changes
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, idleEventID); err != nil {
		t.Fatalf("remove lagging idle stream fixture: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `DELETE FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, idleEventID); err != nil {
		t.Fatalf("remove lagging idle Event fixture: %v", err)
	}

	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET config_generation=2, updated_at=clock_timestamp()
		WHERE workspace_id='default' AND id=$1`, sessionID); err != nil {
		t.Fatalf("advance config generation during hot Thread run: %v", err)
	}
	configJobID := queue.NewJobID()
	if _, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
		ID: configJobID, WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeConfigUpdate,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:      queue.FormatRuntimeConfigUpdateDedupeKey(workspace.DefaultID, sessionID, "2"),
		PayloadVersion: 2,
		PayloadJSON:    []byte(`{"workspace_id":"default","session_id":"` + sessionID + `","config_generation":2}`),
		MaxAttempts:    queue.DefaultMaxAttempts, Now: time.Now().UTC().Add(-time.Second),
	}); err != nil {
		t.Fatalf("enqueue config during hot Thread run: %v", err)
	}
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("defer config against hot Thread = active:%t err:%v", active, err)
	}
	var configStatus string
	var configAttempts int
	if err := admin.QueryRowContext(context.Background(), `SELECT status, attempt_count FROM queue_jobs
		WHERE workspace_id='default' AND id=$1`, configJobID).Scan(&configStatus, &configAttempts); err != nil {
		t.Fatalf("read deferred config boundary: %v", err)
	}
	if configStatus != queue.StatusPending || configAttempts != 0 {
		t.Fatalf("deferred config boundary = %s/%d; want pending/0", configStatus, configAttempts)
	}

	eventService := sessionevent.NewService(sessionevent.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime)))
	interruptBirth, err := eventService.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID,
		"interrupt-production-composition", sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{
			Type: sessionevent.EventTypeUserInterrupt, SessionThreadID: threadID,
		}}})
	if err != nil || len(interruptBirth.Data) != 1 {
		t.Fatalf("birth interrupt through SessionEvent owner = %#v/%v", interruptBirth, err)
	}
	interruptEvent := interruptBirth.Data[0].ID
	var interruptID string
	if err := admin.QueryRowContext(context.Background(), `SELECT runtime_input_id
		FROM session_runtime_inbox
		WHERE workspace_id='default' AND session_id=$1 AND input_kind='interrupt_control'
		  AND event_ids_json::jsonb ? $2`, sessionID, interruptEvent).Scan(&interruptID); err != nil {
		t.Fatalf("read production-born interrupt Inbox: %v", err)
	}
	postConfigEventID := "evt_interrupt_production_post_config"
	postConfigSequence := nextBridgeAPIEventSequenceForTest(t, admin, sessionID, threadID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, postConfigEventID, postConfigSequence, "user.message", `{"content":[{"type":"text","text":"post-config ordinary"}]}`)
	postConfigJob := RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: "rin_interrupt_production_post_config", InputKind: "messages", EventIDs: []string{postConfigEventID},
		SequenceFrom: postConfigSequence, SequenceTo: postConfigSequence,
		PayloadJSON: `{"workspace_id":"default","session_id":"` + sessionID + `","session_thread_id":"` + threadID + `","runtime_input_id":"rin_interrupt_production_post_config","event_ids":["` + postConfigEventID + `"],"sequence_from":` + strconv.FormatInt(postConfigSequence, 10) + `,"sequence_to":` + strconv.FormatInt(postConfigSequence, 10) + `,"input_kind":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, postConfigJob)
	enqueueRuntimeCompositionJob(t, queueStore, sessionID, postConfigJob, 0)
	var ordinaryQueueStatus, ordinaryInboxStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$1),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$2)`,
		queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, postConfigJob.RuntimeInputID), postConfigJob.RuntimeInputID,
	).Scan(&ordinaryQueueStatus, &ordinaryInboxStatus); err != nil {
		t.Fatalf("read post-interrupt config follower: %v", err)
	}
	if ordinaryQueueStatus != queue.StatusPending || ordinaryInboxStatus != "queued" {
		t.Fatalf("post-interrupt config follower = %s/%s; want pending/queued", ordinaryQueueStatus, ordinaryInboxStatus)
	}

	type deliveryResult struct {
		active bool
		err    error
	}
	delivery := make(chan deliveryResult, 1)
	go func() {
		active, runErr := runner.RunOnceWithActivity(context.Background())
		delivery <- deliveryResult{active: active, err: runErr}
	}()
	waitForCompositionFile(t, paths.operationCompleted, "interrupted durable operation completion", &runtimeProcess.output)
	select {
	case premature := <-delivery:
		t.Fatalf("interrupt delivery returned before the blocked output-capture boundary completed: active:%t err:%v", premature.active, premature.err)
	default:
	}

	registry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{
		"daytona": &bridgeMemoryProjectionProvider{},
	})
	if err != nil {
		t.Fatalf("build output capture provider registry: %v", err)
	}
	captureRunner := &tetralsandbox.SandboxOutputCaptureJobRunner{
		Queue:     tetralqueue.NewServer(queueStore, nil),
		Store:     tetralsandbox.NewPostgreSQLSandboxOutputCaptureStore(dbconnect.NewClientForTesting(runtime)),
		Providers: registry,
		BlobStore: blob.NewFakeBlobStore(),
		Config: tetralsandbox.SandboxOutputCaptureRunnerConfig{
			WorkspaceID: "default", LeaseOwner: "interrupt-production-output-capture", MaxJobs: 1,
			LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second,
		},
	}
	captureDeadline := time.Now().Add(10 * time.Second)
	for {
		active, captureErr := captureRunner.RunOnceWithActivity(context.Background())
		if captureErr != nil {
			t.Fatalf("complete interrupt output capture operation: %v", captureErr)
		}
		if active {
			break
		}
		if time.Now().After(captureDeadline) {
			t.Fatalf("interrupt closeout did not enqueue output capture: %s", runtimeProcess.output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	var delivered deliveryResult
	select {
	case delivered = <-delivery:
	case <-time.After(10 * time.Second):
		t.Fatalf("interrupt delivery did not finish after output capture: %s", runtimeProcess.output.String())
	}
	if delivered.err != nil || !delivered.active {
		t.Fatalf("deliver interrupt through Runtime closeout = active:%t err:%v", delivered.active, delivered.err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs SET available_at=clock_timestamp()-interval '1 second'
		WHERE workspace_id='default' AND id=$1 AND status='pending'`, configJobID); err != nil {
		t.Fatalf("make deferred config eligible after interrupt: %v", err)
	}
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("install deferred config after interrupt = active:%t err:%v", active, err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$1`, configJobID).Scan(&configStatus); err != nil || configStatus != queue.StatusAcknowledged {
		t.Fatalf("installed config Queue state = %s/%v; want acknowledged", configStatus, err)
	}
	coldAfterInterrupt, err := bridgeStore.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope: bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID),
	})
	if err != nil {
		t.Fatalf("load cold Provider context after interrupt: %v", err)
	}
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("deliver ordinary input after config install = active:%t err:%v", active, err)
	}
	postConfigDeadline := time.Now().Add(10 * time.Second)
	for {
		var ends int
		if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
			WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_end'`, sessionID, threadID).Scan(&ends); err != nil {
			t.Fatalf("read post-config Request End: %v", err)
		}
		if ends == 2 {
			break
		}
		if time.Now().After(postConfigDeadline) {
			t.Fatalf("ordinary input did not resume after config install: %s", runtimeProcess.output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	postConfigCaptureDeadline := time.Now().Add(10 * time.Second)
	for {
		active, captureErr := captureRunner.RunOnceWithActivity(context.Background())
		if captureErr != nil {
			t.Fatalf("complete post-config output capture: %v", captureErr)
		}
		if active {
			break
		}
		if time.Now().After(postConfigCaptureDeadline) {
			t.Fatalf("post-config completion did not enqueue output capture: %s", runtimeProcess.output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := os.WriteFile(paths.close, []byte("close"), 0o600); err != nil {
		t.Fatalf("release Runtime composition: %v", err)
	}
	composed := runtimeProcess.wait(t)
	if composed.ProviderInvocations != 2 || len(composed.ProviderContexts) != 2 || composed.DurableOperationCompletions != 1 || len(composed.InterruptResult) == 0 {
		t.Fatalf("Runtime composition = %+v; want interrupted and resumed Provider calls, one joined operation, and interrupt response", composed)
	}
	hotProviderContext := composed.ProviderContexts[1]
	for _, providerRequest := range runRuntimeProviderComposition(t, coldAfterInterrupt.GetContextJson()) {
		var coldRequest struct {
			Context json.RawMessage `json:"context"`
		}
		if err := json.Unmarshal(providerRequest, &coldRequest); err != nil {
			t.Fatalf("decode cold Provider request: %v", err)
		}
		var hotValue, coldValue []any
		if err := json.Unmarshal(hotProviderContext, &hotValue); err != nil {
			t.Fatalf("decode hot Provider context: %v", err)
		}
		if err := json.Unmarshal(coldRequest.Context, &coldValue); err != nil {
			t.Fatalf("decode cold Provider context: %v", err)
		}
		if len(hotValue) == 0 {
			t.Fatal("hot Provider context omitted the accepted follower input")
		}
		followerJSON, _ := json.Marshal(hotValue[len(hotValue)-1])
		if !strings.Contains(string(followerJSON), "post-config ordinary") {
			t.Fatalf("hot Provider context did not end with the accepted follower: %s", followerJSON)
		}
		hotJSON, _ := json.Marshal(hotValue[:len(hotValue)-1])
		coldJSON, _ := json.Marshal(coldValue)
		if !bytes.Equal(hotJSON, coldJSON) {
			t.Fatalf("interrupt hot/cold Provider context diverged:\nhot:  %s\ncold: %s", hotJSON, coldJSON)
		}
		assertInterruptedProviderContext(
			t,
			coldRequest.Context,
			"failed interrupt partial text",
			"interrupt Tool reasoning",
			"sig_interrupt_composition",
			"call_interrupt_composition",
		)
	}
	assertInterruptedProviderContext(
		t,
		hotProviderContext,
		"failed interrupt partial text",
		"interrupt Tool reasoning",
		"sig_interrupt_composition",
		"call_interrupt_composition",
	)

	var inboxStatus, queueStatus string
	var starts, ends, toolUses, toolResults, receipts int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$2),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND type='span.model_request_start'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND type='span.model_request_end'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND type='agent.tool_use'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND type='agent.tool_result'),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$3
		 AND operation IN ('commit_inputs','write_request_end') AND source_kind='interrupt_control' AND idempotency_key=$1 AND receipt_json <> '')`,
		interruptID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, interruptID), sessionID,
	).Scan(&inboxStatus, &queueStatus, &starts, &ends, &toolUses, &toolResults, &receipts); err != nil {
		t.Fatalf("read interrupt production closeout: %v", err)
	}
	if inboxStatus != "committed" || queueStatus != queue.StatusAcknowledged || starts != 2 || ends != 2 || toolUses != 1 || toolResults != 1 || receipts != 1 {
		t.Fatalf("production closeout = Inbox:%s Queue:%s starts:%d ends:%d tool uses/results:%d/%d receipts:%d",
			inboxStatus, queueStatus, starts, ends, toolUses, toolResults, receipts)
	}
}

func TestPostgreSQLRecoveredOpenRequestJoinedReplayCompletesResidentFence(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_interrupt_joined_replay"
		threadID       = "thr_interrupt_joined_replay"
		bindingID      = "bind_interrupt_joined_replay"
		podUID         = "pod_interrupt_joined_replay"
		turnID         = "evt_interrupt_joined_replay_running"
		modelRequestID = "mreq_interrupt_joined_replay"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	client := dbconnect.NewClientForTesting(runtime)
	bridgeStore := NewPostgreSQLBridgeAPIStore(client)
	bridgeStore.RuntimeBindingTokenHMACKey = []byte("interrupt-joined-replay-key")
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIOpenDurableTurn(t, admin, scope, turnID)
	seedBridgeAPIRequestStart(t, bridgeStore, scope, "rwrite_"+modelRequestID+"_start", modelRequestID, requestKindAgentProviderRequest, 0)
	partial, err := bridgeStore.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_" + modelRequestID + "_partial", ModelRequestId: modelRequestID,
		EventType: "agent.message", PayloadJson: `{"type":"agent.message","content":[{"type":"text","text":"failed partial text"}]}`,
		AssistantContextDelta: bridgeTextContextDeltaForTest("failed partial text"),
	})
	if err != nil || partial.GetCommitted() == nil {
		t.Fatalf("write joined replay failed partial: response=%#v err=%v", partial, err)
	}
	toolUse, err := bridgeStore.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_" + modelRequestID + "_tool", ModelRequestId: modelRequestID,
		ToolDeclaration: bridgeSignedReasoningToolDeclarationForTest(
			"call_interrupt_joined_replay", "Read", `{"path":"joined.txt"}`, "allow",
		),
	})
	if err != nil || toolUse.GetCommitted() == nil {
		t.Fatalf("write joined replay Tool use: response=%#v err=%v", toolUse, err)
	}
	toolUseEventID := toolUse.GetCommitted().GetEventId()

	eventService := sessionevent.NewService(sessionevent.NewPostgreSQLStore(client))
	birth, err := eventService.AppendClientEvents(
		context.Background(),
		workspace.DefaultID,
		sessionID,
		"interrupt-joined-replay",
		sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{Type: sessionevent.EventTypeUserInterrupt}}},
	)
	if err != nil || len(birth.Data) != 1 {
		t.Fatalf("birth joined replay interrupt = %#v/%v", birth, err)
	}
	var interruptID string
	if err := admin.QueryRowContext(context.Background(), `SELECT runtime_input_id
		FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1
		AND input_kind='interrupt_control'`, sessionID).Scan(&interruptID); err != nil {
		t.Fatalf("read joined replay interrupt identity: %v", err)
	}
	bridgeListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for joined replay Bridge: %v", err)
	}
	bridgeServer := grpc.NewServer()
	RegisterBridgeAPI(bridgeServer, bridgeStore)
	go func() { _ = bridgeServer.Serve(bridgeListener) }()
	t.Cleanup(func() {
		bridgeServer.Stop()
		_ = bridgeListener.Close()
	})
	runtimeProcess, paths := startInterruptRuntimeComposition(
		t,
		t.TempDir(),
		bridgeListener.Addr().String(),
		sessionID,
		threadID,
		bindingID,
		1,
		podUID,
		interruptRuntimeCompositionOptions{failFirstFinishIdle: true},
	)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_bindings
		SET agent_runtime_pod_ip='127.0.0.1'
		WHERE workspace_id='default' AND session_id=$1 AND binding_id=$2`, sessionID, bindingID); err != nil {
		t.Fatalf("align joined replay Runtime binding: %v", err)
	}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(client, runtimeProcess.port)
	deliveryStore.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: podUID, PodIP: "127.0.0.1",
		}})
	}}
	baseSender := NewRuntimePodCommandClient(taskNotificationRuntimeTokenSource{})
	captureSender := &interruptRequestCaptureSender{RuntimeCommandSender: baseSender}
	queueStore := queue.NewPostgreSQLStore(client)
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: captureSender},
		Config: JobRunnerConfig{LeaseOwner: "interrupt-joined-replay", MaxJobs: 1,
			LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("deliver joined replay interrupt = active:%t err:%v", active, err)
	}
	if captureSender.request == nil {
		t.Fatal("joined replay did not reach Runtime")
	}
	replayContext, cancelReplay := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelReplay()
	replay, err := baseSender.Interrupt(replayContext, captureSender.target, captureSender.request)
	if err != nil || replay.GetDuplicate() == nil {
		if closeErr := os.WriteFile(paths.close, []byte("close"), 0o600); closeErr != nil {
			t.Fatalf("redeliver joined interrupt = %#v/%v and close Runtime: %v", replay, err, closeErr)
		}
		failed := runtimeProcess.wait(t)
		t.Fatalf("redeliver joined interrupt = %#v rejected=%#v err=%v output=%+v; want duplicate", replay, replay.GetRejected(), err, failed)
	}
	replayDeadline := time.Now().Add(3 * time.Second)
	for {
		active, runErr := runner.RunOnceWithActivity(context.Background())
		if runErr != nil {
			t.Fatalf("replay joined receipt through JobRunner: %v", runErr)
		}
		if active {
			break
		}
		if time.Now().After(replayDeadline) {
			t.Fatal("joined receipt did not become eligible for Queue replay")
		}
		time.Sleep(25 * time.Millisecond)
	}
	registry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{
		"daytona": &bridgeMemoryProjectionProvider{},
	})
	if err != nil {
		t.Fatalf("build joined replay output capture provider registry: %v", err)
	}
	captureRunner := &tetralsandbox.SandboxOutputCaptureJobRunner{
		Queue:     tetralqueue.NewServer(queueStore, nil),
		Store:     tetralsandbox.NewPostgreSQLSandboxOutputCaptureStore(client),
		Providers: registry,
		BlobStore: blob.NewFakeBlobStore(),
		Config: tetralsandbox.SandboxOutputCaptureRunnerConfig{
			WorkspaceID: "default", LeaseOwner: "interrupt-joined-replay-output-capture", MaxJobs: 1,
			LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second,
		},
	}
	captureDeadline := time.Now().Add(10 * time.Second)
	for {
		active, captureErr := captureRunner.RunOnceWithActivity(context.Background())
		if captureErr != nil {
			t.Fatalf("complete joined replay output capture operation: %v", captureErr)
		}
		if active {
			break
		}
		if time.Now().After(captureDeadline) {
			t.Fatalf("joined replay did not enqueue output capture: %s", runtimeProcess.output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	idleDeadline := time.Now().Add(10 * time.Second)
	for {
		var idleEvents int
		if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
			WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='session.status_idle'`,
			sessionID, threadID).Scan(&idleEvents); err != nil {
			t.Fatalf("read joined replay idle completion: %v", err)
		}
		if idleEvents == 1 {
			break
		}
		if time.Now().After(idleDeadline) {
			t.Fatalf("joined replay did not finish idle after output capture: %s", runtimeProcess.output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := os.WriteFile(paths.close, []byte("close"), 0o600); err != nil {
		t.Fatalf("release joined replay Runtime: %v", err)
	}
	composed := runtimeProcess.wait(t)
	if composed.ProviderInvocations != 0 || composed.FinishIdleInvocations != 2 || len(composed.InterruptResults) != 2 {
		t.Fatalf("joined replay Runtime output = %+v; want zero Provider, one failed and one committed FinishIdle, two interrupts", composed)
	}
	var snapshot struct {
		Ok       bool              `json:"ok"`
		Observed bool              `json:"observed"`
		Status   string            `json:"status"`
		Entries  []json.RawMessage `json:"entries"`
	}
	if err := json.Unmarshal(composed.ThreadSnapshot, &snapshot); err != nil || !snapshot.Ok || !snapshot.Observed {
		t.Fatalf("joined replay resident snapshot = %s/%v", composed.ThreadSnapshot, err)
	}
	joinedCold, err := bridgeStore.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("load joined replay durable projection: %v", err)
	}
	hotEntries, _ := json.Marshal(snapshot.Entries)
	assertInterruptedResidentContext(
		t,
		hotEntries,
		"failed partial text",
		"provider-declared reasoning",
		"sig_provider_context",
		"call_interrupt_joined_replay",
	)
	for _, providerRequest := range runRuntimeProviderComposition(t, joinedCold.GetContextJson()) {
		var coldRequest struct {
			Context json.RawMessage `json:"context"`
		}
		if err := json.Unmarshal(providerRequest, &coldRequest); err != nil {
			t.Fatalf("decode joined replay cold Provider request: %v", err)
		}
		assertInterruptedProviderContext(
			t,
			coldRequest.Context,
			"failed partial text",
			"provider-declared reasoning",
			"sig_provider_context",
			"call_interrupt_joined_replay",
		)
	}
	var inboxStatus, queueStatus string
	var requestEnds, toolResults, receipts, idleEvents int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$2),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3
		 AND session_thread_id=$4 AND type='span.model_request_end' AND model_request_id=$5),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3
		 AND session_thread_id=$4 AND type='agent.tool_result'
		 AND COALESCE(payload_json::jsonb->>'tool_use_event_id', payload_json::jsonb->>'tool_use_id')=$6),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$3
		 AND source_kind='interrupt_control' AND idempotency_key=$1 AND receipt_json <> ''),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3
		 AND session_thread_id=$4 AND type='session.status_idle')`,
		interruptID,
		queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, interruptID),
		sessionID,
		threadID,
		modelRequestID,
		toolUseEventID,
	).Scan(&inboxStatus, &queueStatus, &requestEnds, &toolResults, &receipts, &idleEvents); err != nil {
		t.Fatalf("read joined replay durable facts: %v", err)
	}
	if inboxStatus != "committed" || queueStatus != queue.StatusAcknowledged || requestEnds != 1 ||
		toolResults != 1 || receipts != 1 || idleEvents != 1 {
		t.Fatalf("joined replay facts = Inbox:%s Queue:%s End:%d ToolResult:%d receipt:%d idle:%d",
			inboxStatus, queueStatus, requestEnds, toolResults, receipts, idleEvents)
	}
}

func TestPostgreSQLBareInterruptColdRecoversMainWithPublicAndReviewerSiblings(t *testing.T) {
	runPostgreSQLColdInterruptProductionCase(t, false)
}

func TestPostgreSQLExplicitChildInterruptColdRecoversOnlyChild(t *testing.T) {
	runPostgreSQLColdInterruptProductionCase(t, true)
}

func TestPostgreSQLPodLossContinuesSameInterruptThroughReplacementRuntime(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID     = "sesn_interrupt_pod_loss_production"
		threadID      = "thr_interrupt_pod_loss_production"
		siblingID     = "thr_interrupt_pod_loss_sibling"
		mainTurnID    = "evt_interrupt_pod_loss_main_running"
		siblingTurnID = "evt_interrupt_pod_loss_sibling_running"
		mainRequestID = "mreq_interrupt_pod_loss_main"
		siblingReqID  = "mreq_interrupt_pod_loss_sibling"
		oldBindingID  = "bind_interrupt_pod_loss_old"
		oldPodUID     = "pod_interrupt_pod_loss_old"
		newPodUID     = "pod_interrupt_pod_loss_new"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, threadID, siblingID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, oldBindingID, 1, oldPodUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, oldBindingID, 1)
	client := dbconnect.NewClientForTesting(runtime)
	bridgeStore := NewPostgreSQLBridgeAPIStore(client)
	mainScope := bridgeAPIScope(sessionID, threadID, oldBindingID, 1, oldPodUID)
	siblingScope := bridgeAPIScope(sessionID, siblingID, oldBindingID, 1, oldPodUID)
	seedBridgeAPIOpenDurableTurn(t, admin, mainScope, mainTurnID)
	seedBridgeAPIOpenDurableTurn(t, admin, siblingScope, siblingTurnID)
	mainToolID := writeDurableOrdinaryToolUseForTest(t, bridgeStore, mainScope, mainRequestID,
		"call_interrupt_pod_loss_main", "Read", `{"path":"main.txt"}`)
	seedBridgeAPIRequestStart(t, bridgeStore, siblingScope, "rwrite_"+siblingReqID+"_start", siblingReqID, "agent_provider_request", 0)
	eventService := sessionevent.NewService(sessionevent.NewPostgreSQLStore(client))
	birth, err := eventService.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, "interrupt-pod-loss-production",
		sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{Type: sessionevent.EventTypeUserInterrupt}}})
	if err != nil || len(birth.Data) != 1 {
		t.Fatalf("birth pre-loss interrupt = %#v/%v", birth, err)
	}
	var interruptID, jobID string
	var attemptsBefore int
	if err := admin.QueryRowContext(context.Background(), `SELECT inbox.runtime_input_id, job.id, job.attempt_count
		FROM session_runtime_inbox inbox JOIN queue_jobs job
		  ON job.workspace_id=inbox.workspace_id AND job.dedupe_key='runtime_input:' || inbox.workspace_id || ':' || inbox.session_id || ':' || inbox.runtime_input_id
		WHERE inbox.workspace_id='default' AND inbox.session_id=$1 AND inbox.input_kind='interrupt_control'`, sessionID).
		Scan(&interruptID, &jobID, &attemptsBefore); err != nil {
		t.Fatalf("read pre-loss interrupt identity: %v", err)
	}

	repairStore := runtimePodLossSweepStore(runtime, nil, func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	})
	if repaired, err := repairStore.RepairLostRuntimeBindings(context.Background(), workspace.DefaultID.String()); err != nil || repaired != 1 {
		t.Fatalf("repair pod loss under interrupt = %d/%v", repaired, err)
	}
	var mainEndsAfterLoss, mainResultsAfterLoss, siblingEndsAfterLoss, oldBindings int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		 AND type='span.model_request_end' AND model_request_id=$4),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		 AND type='agent.tool_result' AND payload_json::jsonb->>'tool_use_event_id'=$5),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$3
		 AND type='span.model_request_end' AND model_request_id=$6),
		(SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1)`,
		sessionID, threadID, siblingID, mainRequestID, mainToolID, siblingReqID,
	).Scan(&mainEndsAfterLoss, &mainResultsAfterLoss, &siblingEndsAfterLoss, &oldBindings); err != nil {
		t.Fatalf("read pod-loss Thread partition: %v", err)
	}
	if mainEndsAfterLoss != 0 || mainResultsAfterLoss != 0 || siblingEndsAfterLoss != 1 || oldBindings != 0 {
		t.Fatalf("pod-loss partition = main %d/%d sibling ends %d bindings %d; want 0/0 1 0",
			mainEndsAfterLoss, mainResultsAfterLoss, siblingEndsAfterLoss, oldBindings)
	}
	var recoveryJobs int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM queue_jobs
		WHERE workspace_id='default' AND kind=$1 AND dedupe_key=$2 AND status='pending'`,
		queue.KindRuntimeRecovery,
		queue.FormatRuntimeRecoveryDedupeKey(workspace.DefaultID, sessionID, mainToolID),
	).Scan(&recoveryJobs); err != nil {
		t.Fatalf("read interrupted Thread recovery custody: %v", err)
	}
	if recoveryJobs != 1 {
		t.Fatalf("interrupted Thread recovery jobs = %d; want one exact pending owner", recoveryJobs)
	}
	bridgeStore.RuntimeBindingTokenHMACKey = []byte("interrupt-pod-loss-production-key")
	bridgeListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for pod-loss Bridge: %v", err)
	}
	bridgeServer := grpc.NewServer()
	RegisterBridgeAPI(bridgeServer, bridgeStore)
	go func() { _ = bridgeServer.Serve(bridgeListener) }()
	t.Cleanup(func() {
		bridgeServer.Stop()
		_ = bridgeListener.Close()
	})
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(client, 9090)
	deliveryStore.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: newPodUID, PodIP: "127.0.0.1",
		}})
	}}
	queueStore := queue.NewPostgreSQLStore(client)
	baseSender := NewRuntimePodCommandClient(taskNotificationRuntimeTokenSource{})
	recoverySender := &blockingRecoveryCommandSender{
		RuntimeCommandSender: baseSender,
		recovery:             baseSender,
		planned:              make(chan *agentruntimev1.RecoverThreadRequest, 1),
		release:              make(chan struct{}),
	}
	responseLossSender := &interruptResponseLossSender{
		RuntimeCommandSender: recoverySender,
	}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: responseLossSender},
		Config:    JobRunnerConfig{LeaseOwner: "interrupt-pod-loss-replacement", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	type deliveryResult struct {
		active bool
		err    error
	}
	delivery := make(chan deliveryResult, 1)
	go func() {
		active, runErr := runner.RunOnceWithActivity(context.Background())
		delivery <- deliveryResult{active: active, err: runErr}
	}()
	var recoveryRequest *agentruntimev1.RecoverThreadRequest
	select {
	case recoveryRequest = <-recoverySender.planned:
	case unexpected := <-delivery:
		t.Fatalf("first post-loss Queue job bypassed recovery = active:%t err:%v", unexpected.active, unexpected.err)
	case <-time.After(10 * time.Second):
		t.Fatal("runtime recovery lease did not install the replacement Runtime")
	}
	if recoveryRequest.GetSourceEventId() != mainToolID || recoveryRequest.GetRecoveryLeaseRef() == nil {
		t.Fatalf("replacement recovery authority = source:%s lease:%#v; want %s and exact lease",
			recoveryRequest.GetSourceEventId(), recoveryRequest.GetRecoveryLeaseRef(), mainToolID)
	}
	runtimeProcess, paths := startInterruptRuntimeComposition(
		t,
		t.TempDir(),
		bridgeListener.Addr().String(),
		sessionID,
		threadID,
		recoveryRequest.GetBindingId(),
		recoveryRequest.GetBindingGeneration(),
		recoveryRequest.GetTargetPodUid(),
	)
	deliveryStore.RuntimeGRPCPort = runtimeProcess.port
	recoverySender.targetPort.Store(int32(runtimeProcess.port))
	close(recoverySender.release)
	select {
	case recovered := <-delivery:
		if recovered.err != nil || !recovered.active {
			t.Fatalf("recover interrupted Thread under exact lease = active:%t err:%v", recovered.active, recovered.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("runtime recovery delivery did not finish: %s", runtimeProcess.output.String())
	}
	var recoveryQueueStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM queue_jobs
		WHERE workspace_id='default' AND kind=$1 AND dedupe_key=$2`,
		queue.KindRuntimeRecovery,
		queue.FormatRuntimeRecoveryDedupeKey(workspace.DefaultID, sessionID, mainToolID),
	).Scan(&recoveryQueueStatus); err != nil {
		t.Fatalf("read recovery Queue settlement: %v", err)
	}
	if recoveryQueueStatus != queue.StatusAcknowledged {
		t.Fatalf("recovery Queue status = %s; want acknowledged before interrupt delivery", recoveryQueueStatus)
	}
	replacementScope := bridgeAPIScope(
		sessionID,
		threadID,
		recoveryRequest.GetBindingId(),
		recoveryRequest.GetBindingGeneration(),
		recoveryRequest.GetTargetPodUid(),
	)
	delivery = make(chan deliveryResult, 1)
	go func() {
		active, runErr := runner.RunOnceWithActivity(context.Background())
		delivery <- deliveryResult{active: active, err: runErr}
	}()
	registry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{
		"daytona": &bridgeMemoryProjectionProvider{},
	})
	if err != nil {
		t.Fatalf("build pod-loss output capture provider registry: %v", err)
	}
	captureRunner := &tetralsandbox.SandboxOutputCaptureJobRunner{
		Queue:     tetralqueue.NewServer(queueStore, nil),
		Store:     tetralsandbox.NewPostgreSQLSandboxOutputCaptureStore(dbconnect.NewClientForTesting(runtime)),
		Providers: registry,
		BlobStore: blob.NewFakeBlobStore(),
		Config: tetralsandbox.SandboxOutputCaptureRunnerConfig{
			WorkspaceID: "default", LeaseOwner: "interrupt-pod-loss-output-capture", MaxJobs: 1,
			LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second,
		},
	}
	captureDeadline := time.Now().Add(10 * time.Second)
	for {
		active, captureErr := captureRunner.RunOnceWithActivity(context.Background())
		if captureErr != nil {
			t.Fatalf("complete pod-loss interrupt output capture: %v", captureErr)
		}
		if active {
			break
		}
		if time.Now().After(captureDeadline) {
			t.Fatalf("pod-loss interrupt did not enqueue output capture: %s", runtimeProcess.output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	var delivered deliveryResult
	select {
	case delivered = <-delivery:
	case <-time.After(10 * time.Second):
		t.Fatalf("pod-loss interrupt delivery did not finish after output capture: %s", runtimeProcess.output.String())
	}
	if delivered.err != nil || !delivered.active {
		t.Fatalf("deliver same interrupt after pod loss = active:%t err:%v", delivered.active, delivered.err)
	}
	if calls := responseLossSender.calls.Load(); calls != 1 {
		t.Fatalf("replacement Runtime interrupt calls after response loss = %d; want 1", calls)
	}
	waitForThreadRequestEnds(t, admin, sessionID, threadID, 1)
	if err := os.WriteFile(paths.close, []byte("close"), 0o600); err != nil {
		t.Fatalf("release pod-loss Runtime composition: %v", err)
	}
	composed := runtimeProcess.wait(t)
	if len(composed.InterruptResult) == 0 || composed.ProviderInvocations != 0 {
		t.Fatalf("pod-loss interrupt Runtime composition = %+v", composed)
	}
	var finalInbox, finalQueue string
	var attemptsAfter, lineage, receipts, activeBarriers, mainEnds, mainResults, danglingTools int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$3),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$4
		 AND source_kind='interrupt_control' AND idempotency_key=$1 AND receipt_json <> ''),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$4
		 AND input_kind='interrupt_control' AND status IN ('queued','delivering','accepted')),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$4 AND session_thread_id=$5
		 AND type='span.model_request_end'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$4 AND session_thread_id=$5
		 AND type='agent.tool_result' AND (payload_json::jsonb->>'tool_use_event_id'=$6 OR payload_json::jsonb->>'tool_use_id'=$6)),
		(SELECT count(*) FROM session_events tool_use WHERE tool_use.workspace_id='default' AND tool_use.session_id=$4
		 AND tool_use.session_thread_id=$5 AND tool_use.type IN ('agent.tool_use','agent.mcp_tool_use')
		 AND NOT EXISTS (SELECT 1 FROM session_events result WHERE result.workspace_id=tool_use.workspace_id
		  AND result.session_id=tool_use.session_id AND result.session_thread_id=tool_use.session_thread_id
		  AND ((result.type='agent.tool_result' AND (result.payload_json::jsonb->>'tool_use_event_id'=tool_use.event_id
		    OR result.payload_json::jsonb->>'tool_use_id'=tool_use.event_id))
		    OR (result.type='agent.mcp_tool_result' AND result.payload_json::jsonb->>'mcp_tool_use_id'=tool_use.event_id))))`,
		interruptID, jobID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, interruptID), sessionID,
		threadID, mainToolID).
		Scan(&finalInbox, &finalQueue, &attemptsAfter, &lineage, &receipts, &activeBarriers, &mainEnds, &mainResults, &danglingTools); err != nil {
		t.Fatalf("read pod-loss interrupt terminal facts: %v", err)
	}
	if finalInbox != "committed" || finalQueue != queue.StatusAcknowledged || attemptsAfter != attemptsBefore+1 || lineage != 1 ||
		receipts != 1 || activeBarriers != 0 || mainEnds != 1 || mainResults != 1 || danglingTools != 0 {
		t.Fatalf("pod-loss terminal facts = Inbox:%s Queue:%s attempts:%d->%d lineage:%d receipts:%d barriers:%d main:%d/%d dangling:%d",
			finalInbox, finalQueue, attemptsBefore, attemptsAfter, lineage, receipts, activeBarriers, mainEnds, mainResults, danglingTools)
	}
	reloaded, err := bridgeStore.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: replacementScope})
	if err != nil {
		t.Fatalf("cold reload completed pod-loss interrupt: %v", err)
	}
	var cold bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(reloaded.GetContextJson()), &cold); err != nil {
		t.Fatalf("decode completed pod-loss interrupt context: %v", err)
	}
	var coldStarts, coldEnds, coldToolUses, coldToolResults int
	for _, event := range cold.TurnFacts.Events {
		if event.ModelRequestID != nil && *event.ModelRequestID == mainRequestID {
			if event.RequestStart != nil {
				coldStarts++
			}
			if event.RequestEnd != nil {
				coldEnds++
			}
		}
		if event.EventID == mainToolID && event.ToolUse != nil {
			coldToolUses++
		}
		if event.ToolResult != nil && event.ToolResult.ToolName == "Read" {
			coldToolResults++
		}
	}
	if coldStarts != 1 || coldEnds != 1 || coldToolUses != 1 || coldToolResults != 1 || len(cold.PendingToolUses) != 0 {
		t.Fatalf("cold completed pair = starts:%d ends:%d uses:%d results:%d pending:%d; want 1/1/1/1/0",
			coldStarts, coldEnds, coldToolUses, coldToolResults, len(cold.PendingToolUses))
	}
}

func TestPostgreSQLPodLossAfterInterruptCloseoutReplaysReceiptWithoutRuntime(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_interrupt_closeout_then_loss"
		threadID       = "thr_interrupt_closeout_then_loss"
		bindingID      = "bind_interrupt_closeout_then_loss"
		podUID         = "pod_interrupt_closeout_then_loss"
		newPodUID      = "pod_interrupt_closeout_then_loss_new"
		turnID         = "evt_interrupt_closeout_then_loss_running"
		modelRequestID = "mreq_interrupt_closeout_then_loss"
		interruptID    = "rin_interrupt_closeout_then_loss"
		interruptEvent = "evt_interrupt_closeout_then_loss"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIOpenDurableTurn(t, admin, scope, turnID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_interrupt_closeout_then_loss_start", modelRequestID, requestKindAgentProviderRequest, 0)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, interruptEvent, 3, "user.interrupt", `{}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, interruptID, "interrupt_control",
		`["`+interruptEvent+`"]`, "accepted", bindingID, podUID, 3, 3)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueueInterruptExhaustionJob(t, queueStore, sessionID, threadID, interruptID, "interrupt_control", interruptEvent, 3, 3, time.Now().UTC())
	oldLease := mustLeaseBridgeQueueJob(t, queueStore, queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "interrupt-closeout-old-owner",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC(),
	})
	ended, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_interrupt_closeout_then_loss_end", ModelRequestId: modelRequestID,
		FinishReason: "cancelled", UsageJson: `{}`, IsError: true, ErrorKind: "runtime_interrupted",
		ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "interrupted"},
		InterruptSettlement: &bridgev1.RequestEndInterruptSettlement{
			RuntimeInputId: interruptID, InterruptLeaseRef: bridgeInterruptLeaseRef(oldLease),
		},
	})
	if err != nil || ended.GetCommitted() == nil {
		t.Fatalf("commit interrupt closeout before pod loss = %#v/%v", ended, err)
	}
	var inboxAfterCloseout string
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox
		WHERE workspace_id='default' AND runtime_input_id=$1`, interruptID).Scan(&inboxAfterCloseout); err != nil {
		t.Fatalf("read interrupt Inbox after closeout: %v", err)
	}
	if inboxAfterCloseout != "committed" {
		t.Fatalf("interrupt Inbox after closeout = %s; want committed", inboxAfterCloseout)
	}
	closeoutJob, err := DecodeRuntimeJob(queueJobProto(oldLease))
	if err != nil {
		t.Fatalf("decode committed interrupt closeout job: %v", err)
	}
	if replayed, found, replayErr := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090).
		ReplayRuntimeDeliveryFinalization(context.Background(), closeoutJob); replayErr != nil || found {
		t.Fatalf("replay interrupt receipt while durable Turn is open = %#v/%t/%v; want not found", replayed, found, replayErr)
	}
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	repairStore := runtimePodLossSweepStore(runtime, nil, func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	})
	if repaired, err := repairStore.RepairLostRuntimeBindings(context.Background(), workspace.DefaultID.String()); err != nil || repaired != 1 {
		t.Fatalf("repair pod loss after interrupt closeout = %d/%v", repaired, err)
	}
	var inboxAfterLoss string
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox
		WHERE workspace_id='default' AND runtime_input_id=$1`, interruptID).Scan(&inboxAfterLoss); err != nil {
		t.Fatalf("read interrupt Inbox after pod loss: %v", err)
	}
	if inboxAfterLoss != "committed" {
		t.Fatalf("interrupt Inbox after pod loss = %s; want committed", inboxAfterLoss)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs
		SET leased_until=clock_timestamp()-interval '1 second'
		WHERE workspace_id='default' AND id=$1`, oldLease.ID); err != nil {
		t.Fatalf("expire pre-loss interrupt lease: %v", err)
	}
	if reclaimed, err := queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{
		WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput, Limit: 1,
	}); err != nil || reclaimed != 1 {
		t.Fatalf("reclaim pre-loss interrupt lease = %d/%v", reclaimed, err)
	}
	client := dbconnect.NewClientForTesting(runtime)
	store.RuntimeBindingTokenHMACKey = []byte("interrupt-closeout-then-loss-key")
	bridgeListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for closeout recovery Bridge: %v", err)
	}
	bridgeServer := grpc.NewServer()
	RegisterBridgeAPI(bridgeServer, store)
	go func() { _ = bridgeServer.Serve(bridgeListener) }()
	t.Cleanup(func() {
		bridgeServer.Stop()
		_ = bridgeListener.Close()
	})
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(client, 9090)
	deliveryStore.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: newPodUID, PodIP: "127.0.0.1",
		}})
	}}
	baseSender := NewRuntimePodCommandClient(taskNotificationRuntimeTokenSource{})
	recoverySender := &blockingRecoveryCommandSender{
		RuntimeCommandSender: baseSender,
		recovery:             baseSender,
		planned:              make(chan *agentruntimev1.RecoverThreadRequest, 1),
		release:              make(chan struct{}),
	}
	responseLossSender := &interruptResponseLossSender{RuntimeCommandSender: recoverySender}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: responseLossSender},
		Config: JobRunnerConfig{LeaseOwner: "interrupt-closeout-replay-owner", MaxJobs: 1,
			LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	type deliveryResult struct {
		active bool
		err    error
	}
	recoveryDelivery := make(chan deliveryResult, 1)
	go func() {
		active, runErr := runner.RunOnceWithActivity(context.Background())
		recoveryDelivery <- deliveryResult{active: active, err: runErr}
	}()
	var recoveryRequest *agentruntimev1.RecoverThreadRequest
	select {
	case recoveryRequest = <-recoverySender.planned:
	case unexpected := <-recoveryDelivery:
		t.Fatalf("committed closeout bypassed recovery = active:%t err:%v", unexpected.active, unexpected.err)
	case <-time.After(10 * time.Second):
		t.Fatal("committed closeout did not lease Runtime recovery")
	}
	if recoveryRequest.GetSourceEventId() == "" || recoveryRequest.GetRecoveryLeaseRef() == nil {
		t.Fatalf("closeout recovery authority = source:%q lease:%#v; want Request End source and exact lease",
			recoveryRequest.GetSourceEventId(), recoveryRequest.GetRecoveryLeaseRef())
	}
	runtimeProcess, paths := startInterruptRuntimeComposition(
		t,
		t.TempDir(),
		bridgeListener.Addr().String(),
		sessionID,
		threadID,
		recoveryRequest.GetBindingId(),
		recoveryRequest.GetBindingGeneration(),
		recoveryRequest.GetTargetPodUid(),
	)
	deliveryStore.RuntimeGRPCPort = runtimeProcess.port
	recoverySender.targetPort.Store(int32(runtimeProcess.port))
	close(recoverySender.release)
	select {
	case recovered := <-recoveryDelivery:
		if recovered.err != nil || !recovered.active {
			t.Fatalf("recover committed interrupt closeout = active:%t err:%v", recovered.active, recovered.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("committed closeout recovery did not finish delivery: %s", runtimeProcess.output.String())
	}
	registry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{
		"daytona": &bridgeMemoryProjectionProvider{},
	})
	if err != nil {
		t.Fatalf("build closeout recovery output capture registry: %v", err)
	}
	captureRunner := &tetralsandbox.SandboxOutputCaptureJobRunner{
		Queue:     tetralqueue.NewServer(queueStore, nil),
		Store:     tetralsandbox.NewPostgreSQLSandboxOutputCaptureStore(client),
		Providers: registry,
		BlobStore: blob.NewFakeBlobStore(),
		Config: tetralsandbox.SandboxOutputCaptureRunnerConfig{
			WorkspaceID: "default", LeaseOwner: "interrupt-closeout-recovery-capture", MaxJobs: 1,
			LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second,
		},
	}
	captureDeadline := time.Now().Add(10 * time.Second)
	for {
		active, captureErr := captureRunner.RunOnceWithActivity(context.Background())
		if captureErr != nil {
			t.Fatalf("complete recovered interrupt output capture: %v", captureErr)
		}
		if active {
			break
		}
		if time.Now().After(captureDeadline) {
			t.Fatalf("recovered interrupt closeout did not enqueue output capture: %s", runtimeProcess.output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	idleDeadline := time.Now().Add(10 * time.Second)
	for {
		var idleEvents int
		if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
			WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
			  AND type='session.status_idle' AND sequence > (
			    SELECT sequence FROM session_events WHERE workspace_id='default' AND event_id=$3
			  )`, sessionID, threadID, turnID).Scan(&idleEvents); err != nil {
			t.Fatalf("read recovered idle settlement: %v", err)
		}
		if idleEvents == 1 {
			break
		}
		if time.Now().After(idleDeadline) {
			t.Fatalf("replacement Runtime did not FinishIdle before receipt replay: %s", runtimeProcess.output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := os.WriteFile(paths.close, []byte("close"), 0o600); err != nil {
		t.Fatalf("release closeout recovery Runtime: %v", err)
	}
	composed := runtimeProcess.wait(t)
	if composed.ProviderInvocations != 0 || len(composed.InterruptResult) != 0 {
		t.Fatalf("closeout recovery Runtime activity = %+v; want recovered closeout without Provider or new interrupt", composed)
	}
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("replay interrupt receipt after recovered FinishIdle = active:%t err:%v", active, err)
	}
	if calls := responseLossSender.calls.Load(); calls != 0 {
		t.Fatalf("Runtime interrupt calls after recovered closeout receipt = %d; want 0", calls)
	}
	var queueStatus string
	var requestEnds, receipts, bindings int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$1),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$2
		 AND session_thread_id=$3 AND type='span.model_request_end' AND model_request_id=$4),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$2
		 AND session_thread_id=$3 AND source_kind='interrupt_control' AND idempotency_key=$5 AND receipt_json <> ''),
		(SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$2)`,
		oldLease.ID, sessionID, threadID, modelRequestID, interruptID,
	).Scan(&queueStatus, &requestEnds, &receipts, &bindings); err != nil {
		t.Fatalf("read post-loss receipt replay facts: %v", err)
	}
	if queueStatus != queue.StatusAcknowledged || requestEnds != 1 || receipts != 1 || bindings != 1 {
		t.Fatalf("post-loss receipt replay = Queue:%s ends:%d receipts:%d bindings:%d; want acknowledged/1/1/1",
			queueStatus, requestEnds, receipts, bindings)
	}
}

func TestPostgreSQLInterruptedActorEffectsStayStaleThroughTerminalCloseout(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID     = "sesn_interrupt_actor_production"
		mainID        = "thr_interrupt_actor_production_main"
		siblingID     = "thr_interrupt_actor_production_sibling"
		grandchildID  = "thr_interrupt_actor_production_grandchild"
		bindingID     = "bind_interrupt_actor_production"
		podUID        = "pod_interrupt_actor_production"
		siblingSource = "evt_interrupt_actor_external_mail"
		lateMail      = "evt_interrupt_actor_late_mail_production"
		lateChild     = "evt_interrupt_actor_late_child_production"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, siblingID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, siblingID, grandchildID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	seedBridgeAPIEvent(t, admin, "default", sessionID, mainID, siblingSource, 1, "agent.tool_use",
		`{"type":"agent.tool_use","name":"send_message","input":{"task_name":"task_`+siblingID+`","message":"external sibling mail waits"},"evaluated_permission":"allow"}`)
	seedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, mainID, siblingSource)
	seedBridgeAPIEvent(t, admin, "default", sessionID, siblingID, lateMail, 1, "agent.tool_use",
		`{"type":"agent.tool_use","name":"send_message","input":{"task_name":"task_`+grandchildID+`","message":"must be stale"},"evaluated_permission":"allow"}`)
	seedBridgeAPIEvent(t, admin, "default", sessionID, siblingID, lateChild, 2, "agent.tool_use",
		`{"type":"agent.tool_use","name":"spawn_agent","input":{"task_name":"late-child","prompt":"must be stale"},"evaluated_permission":"allow"}`)
	seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, siblingID, "mreq_interrupt_actor_late", lateMail, "call_interrupt_actor_late_mail", "send_message")
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_messages
		SET data_json=jsonb_set(data_json::jsonb, '{parts}', (data_json::jsonb->'parts') ||
			'[{"type":"tool_call","modelToolCallId":"call_interrupt_actor_late_child","toolName":"spawn_agent","canonicalInput":{}}]'::jsonb)::text
		WHERE workspace_id='default' AND session_id=$1 AND source_event_id=$2`, sessionID, lateMail); err != nil {
		t.Fatalf("add second durable actor Tool part: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events
		SET model_request_id='mreq_interrupt_actor_late', projection_json=COALESCE(projection_json, '{}')::jsonb ||
			'{"model_tool_call_id":"call_interrupt_actor_late_child","tool_name":"spawn_agent"}'::jsonb
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, lateChild); err != nil {
		t.Fatalf("bind second actor Tool identity: %v", err)
	}

	client := dbconnect.NewClientForTesting(runtime)
	eventService := sessionevent.NewService(sessionevent.NewPostgreSQLStore(client))
	birth, err := eventService.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, "interrupt-actor-production",
		sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{Type: sessionevent.EventTypeUserInterrupt, SessionThreadID: siblingID}}})
	if err != nil || len(birth.Data) != 1 {
		t.Fatalf("birth actor interrupt = %#v/%v", birth, err)
	}
	var interruptID string
	if err := admin.QueryRowContext(context.Background(), `SELECT runtime_input_id FROM session_runtime_inbox
		WHERE workspace_id='default' AND session_id=$1 AND input_kind='interrupt_control'`, sessionID).Scan(&interruptID); err != nil {
		t.Fatalf("read actor interrupt identity: %v", err)
	}

	bridgeStore := NewPostgreSQLBridgeAPIStore(client)
	bridgeStore.RuntimeBindingTokenHMACKey = []byte("interrupt-actor-production-key")
	mainScope := bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID)
	siblingScope := bridgeAPIScope(sessionID, siblingID, bindingID, 1, podUID)
	deliveryID := agentMailDeliveryID(siblingSource, siblingID)
	if response, err := bridgeStore.DeliverInterAgentMail(context.Background(), &bridgev1.DeliverInterAgentMailRequest{
		Scope: mainScope, DeliveryId: deliveryID, TargetThreadId: siblingID,
		SourceToolUseEventId: siblingSource, Content: "external sibling mail waits",
	}); err != nil || response.GetCommitted() == nil {
		t.Fatalf("external sibling mail behind barrier = %#v/%v", response, err)
	}
	if _, err := bridgeStore.DeliverInterAgentMail(context.Background(), &bridgev1.DeliverInterAgentMailRequest{
		Scope: siblingScope, DeliveryId: agentMailDeliveryID(lateMail, grandchildID), TargetThreadId: grandchildID,
		SourceToolUseEventId: lateMail, Content: "must be stale",
	}); !isThreadInterruptBarrierStaleError(err) {
		t.Fatalf("interrupted-source mail = %v; want barrier stale", err)
	}
	if _, err := bridgeStore.CreateSubagentThread(context.Background(), &bridgev1.CreateSubagentThreadRequest{
		Scope: siblingScope, SourceToolUseEventId: lateChild, TaskName: "late-child", AgentType: "worker", InitialPrompt: "must be stale",
	}); !isThreadInterruptBarrierStaleError(err) {
		t.Fatalf("interrupted-source child birth = %v; want barrier stale", err)
	}
	// The two rejected Tool sources are timing fixtures, not Runtime history.
	// Remove them after the owning Bridge calls prove zero effects so the cold
	// Runtime closeout loads only durable inputs that survived the barrier.
	if _, err := admin.ExecContext(context.Background(), `DELETE FROM session_messages
		WHERE workspace_id='default' AND session_id=$1 AND source_event_id=$2`, sessionID, lateMail); err != nil {
		t.Fatalf("remove rejected actor Tool message fixture: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `DELETE FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND event_id IN ($2,$3)`, sessionID, lateMail, lateChild); err != nil {
		t.Fatalf("remove rejected actor Tool event fixtures: %v", err)
	}
	if _, err := bridgeStore.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: siblingScope}); err != nil {
		t.Fatalf("load interrupted actor context before Runtime: %v", err)
	}

	bridgeListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for actor Bridge: %v", err)
	}
	bridgeServer := grpc.NewServer()
	RegisterBridgeAPI(bridgeServer, bridgeStore)
	go func() { _ = bridgeServer.Serve(bridgeListener) }()
	t.Cleanup(func() {
		bridgeServer.Stop()
		_ = bridgeListener.Close()
	})
	runtimeProcess, paths := startInterruptRuntimeComposition(t, t.TempDir(), bridgeListener.Addr().String(), sessionID, siblingID, bindingID, 1, podUID)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_bindings SET agent_runtime_pod_ip='127.0.0.1'
		WHERE workspace_id='default' AND session_id=$1 AND binding_id=$2`, sessionID, bindingID); err != nil {
		t.Fatalf("align actor Runtime binding: %v", err)
	}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(client, runtimeProcess.port)
	deliveryStore.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: podUID, PodIP: "127.0.0.1",
		}})
	}}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queue.NewPostgreSQLStore(client), nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: NewRuntimePodCommandClient(taskNotificationRuntimeTokenSource{})},
		Config:    JobRunnerConfig{LeaseOwner: "interrupt-actor-closeout", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("deliver actor interrupt through Runtime = active:%t err:%v", active, err)
	}
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("deliver released external sibling mail through Runtime = active:%t err:%v", active, err)
	}
	if err := os.WriteFile(paths.close, []byte("close"), 0o600); err != nil {
		t.Fatalf("release actor Runtime composition: %v", err)
	}
	composed := runtimeProcess.wait(t)
	if len(composed.InterruptResult) == 0 || composed.ProviderInvocations != 0 {
		t.Fatalf("actor interrupt Runtime composition = %+v", composed)
	}

	mailRuntimeID := completionRuntimeInputID(deliveryID)
	var interruptInbox, interruptQueue, mailInbox, mailQueue string
	var lateOperations, lateChildren, activeBarriers int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$2),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$3),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$4),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$5 AND idempotency_key IN ($6,$7)),
		(SELECT count(*) FROM session_threads WHERE workspace_id='default' AND session_id=$5 AND task_name='late-child'),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$5
		 AND input_kind='interrupt_control' AND status IN ('queued','delivering','accepted'))`,
		interruptID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, interruptID),
		mailRuntimeID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, mailRuntimeID),
		sessionID, agentMailDeliveryID(lateMail, grandchildID), lateChild).
		Scan(&interruptInbox, &interruptQueue, &mailInbox, &mailQueue, &lateOperations, &lateChildren, &activeBarriers); err != nil {
		t.Fatalf("read actor terminal facts: %v", err)
	}
	if interruptInbox != "committed" || interruptQueue != queue.StatusAcknowledged ||
		mailInbox != "committed" || mailQueue != queue.StatusDeadLettered || lateOperations != 0 || lateChildren != 0 || activeBarriers != 0 {
		t.Fatalf("actor terminal facts = interrupt:%s/%s mail:%s/%s late:%d/%d barriers:%d Runtime:%s",
			interruptInbox, interruptQueue, mailInbox, mailQueue, lateOperations, lateChildren, activeBarriers, composed.InterruptResult)
	}
}

func runPostgreSQLColdInterruptProductionCase(t *testing.T, explicitChild bool) {
	t.Helper()
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID  = "sesn_interrupt_cold_production"
		mainID     = "thr_interrupt_cold_main"
		childID    = "thr_interrupt_cold_child"
		reviewerID = "thr_interrupt_cold_reviewer"
		bindingID  = "bind_interrupt_cold_production"
		podUID     = "pod_interrupt_cold_production"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIInternalReviewerThread(t, admin, "default", sessionID, mainID, reviewerID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	seedBridgeAPIEvent(t, admin, "default", sessionID, mainID, "evt_interrupt_cold_history", 1, "user.message", `{"content":[{"type":"text","text":"durable cold history"}]}`)
	seedBridgeAPIProjectedUserMessage(t, admin, sessionID, mainID, "msg_interrupt_cold_history", "evt_interrupt_cold_history", 1)
	runtimeThreadID := mainID
	interruptThreadID := ""
	if explicitChild {
		runtimeThreadID = childID
		interruptThreadID = childID
	}

	bridgeStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridgeStore.RuntimeBindingTokenHMACKey = []byte("interrupt-cold-production-key")
	bridgeListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for cold interrupt Bridge: %v", err)
	}
	bridgeServer := grpc.NewServer()
	RegisterBridgeAPI(bridgeServer, bridgeStore)
	go func() { _ = bridgeServer.Serve(bridgeListener) }()
	t.Cleanup(func() {
		bridgeServer.Stop()
		_ = bridgeListener.Close()
	})

	runtimeProcess, paths := startInterruptRuntimeComposition(t, t.TempDir(), bridgeListener.Addr().String(), sessionID, runtimeThreadID, bindingID, 1, podUID)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_bindings
		SET agent_runtime_pod_ip='127.0.0.1'
		WHERE workspace_id='default' AND session_id=$1 AND binding_id=$2`, sessionID, bindingID); err != nil {
		t.Fatalf("align cold Runtime binding: %v", err)
	}

	eventService := sessionevent.NewService(sessionevent.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime)))
	birth, err := eventService.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID,
		"interrupt-cold-production", sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{Type: sessionevent.EventTypeUserInterrupt, SessionThreadID: interruptThreadID}}})
	if err != nil || len(birth.Data) != 1 {
		t.Fatalf("birth bare cold interrupt = %#v/%v", birth, err)
	}
	var interruptID, targetThreadID string
	var interruptCount, queueCount int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND input_kind='interrupt_control'),
		(SELECT runtime_input_id FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND input_kind='interrupt_control'),
		(SELECT session_thread_id FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND input_kind='interrupt_control'),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND partition_key=$2 AND kind=$3)`,
		sessionID, queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID), queue.KindRuntimeInput,
	).Scan(&interruptCount, &interruptID, &targetThreadID, &queueCount); err != nil {
		t.Fatalf("read bare cold interrupt birth: %v", err)
	}
	if interruptCount != 1 || queueCount != 1 || targetThreadID != runtimeThreadID {
		t.Fatalf("cold interrupt birth = Inbox:%d Queue:%d target:%s; want one/one/%s", interruptCount, queueCount, targetThreadID, runtimeThreadID)
	}

	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), runtimeProcess.port)
	deliveryStore.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: podUID, PodIP: "127.0.0.1",
		}})
	}}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: NewRuntimePodCommandClient(taskNotificationRuntimeTokenSource{})},
		Config:    JobRunnerConfig{LeaseOwner: "interrupt-cold-production", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("deliver cold interrupt through Runtime = active:%t err:%v", active, err)
	}
	if err := os.WriteFile(paths.close, []byte("close"), 0o600); err != nil {
		t.Fatalf("release cold Runtime composition: %v", err)
	}
	composed := runtimeProcess.wait(t)
	if composed.ProviderInvocations != 0 || composed.DurableOperationCompletions != 0 || len(composed.InterruptResult) == 0 {
		t.Fatalf("cold interrupt Runtime composition = %+v; want cold closeout without Provider work", composed)
	}
	var inboxStatus, queueStatus string
	var receipts, activeBarriers int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$2),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$3
		 AND operation IN ('commit_inputs','write_request_end') AND source_kind='interrupt_control' AND idempotency_key=$1 AND receipt_json <> ''),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$3
		 AND input_kind='interrupt_control' AND status IN ('queued','delivering','accepted'))`,
		interruptID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, interruptID), sessionID,
	).Scan(&inboxStatus, &queueStatus, &receipts, &activeBarriers); err != nil {
		t.Fatalf("read cold interrupt terminal facts: %v", err)
	}
	if inboxStatus != "committed" || queueStatus != queue.StatusAcknowledged || receipts != 1 || activeBarriers != 0 {
		t.Fatalf("cold interrupt terminal facts = Inbox:%s Queue:%s receipts:%d barriers:%d", inboxStatus, queueStatus, receipts, activeBarriers)
	}
}

func enqueueRuntimeCompositionJob(t *testing.T, store *queue.PostgreSQLQueueStore, sessionID string, job RuntimeJob, priority int) {
	t.Helper()
	if _, err := store.Enqueue(context.Background(), queue.EnqueueRequest{
		ID: queue.NewJobID(), WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, job.RuntimeInputID),
		PayloadVersion: 1, PayloadJSON: []byte(job.PayloadJSON), Priority: priority,
		MaxAttempts: queue.DefaultMaxAttempts, Now: time.Now().UTC().Add(-time.Second),
	}); err != nil {
		t.Fatalf("enqueue Runtime composition job %s: %v", job.RuntimeInputID, err)
	}
}

func assertInterruptedProviderContext(
	t *testing.T,
	raw json.RawMessage,
	failedText string,
	reasoningText string,
	reasoningSignature string,
	modelToolCallID string,
) {
	t.Helper()
	var entries []struct {
		Content []struct {
			Text *struct {
				Text string `json:"text"`
			} `json:"text"`
			Reasoning *struct {
				Text         string `json:"text"`
				MetadataJSON string `json:"metadataJson"`
			} `json:"reasoning"`
			ToolCall *struct {
				ModelToolCallID string `json:"modelToolCallId"`
			} `json:"toolCall"`
			ToolResult *struct {
				ModelToolCallID string `json:"modelToolCallId"`
			} `json:"toolResult"`
		} `json:"content"`
	}
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatalf("decode interrupted Provider context: %v: %s", err, raw)
	}
	reasoningIndex, toolCallIndex, toolResultIndex := -1, -1, -1
	reasoningCount, toolCallCount, toolResultCount := 0, 0, 0
	partIndex := 0
	for _, entry := range entries {
		for _, part := range entry.Content {
			if part.Text != nil && strings.Contains(part.Text.Text, failedText) {
				t.Fatalf("interrupted Provider context retained failed text %q: %s", failedText, raw)
			}
			if part.Reasoning != nil {
				reasoningCount++
				reasoningIndex = partIndex
				if part.Reasoning.Text != reasoningText {
					t.Fatalf("interrupted Provider reasoning text = %q; want %q", part.Reasoning.Text, reasoningText)
				}
				var metadata any
				if err := json.Unmarshal([]byte(part.Reasoning.MetadataJSON), &metadata); err != nil {
					t.Fatalf("decode interrupted Provider reasoning metadata %q: %v", part.Reasoning.MetadataJSON, err)
				}
				gotMetadata, _ := json.Marshal(metadata)
				wantMetadata, _ := json.Marshal(map[string]any{"anthropic": map[string]any{"signature": reasoningSignature}})
				if !bytes.Equal(gotMetadata, wantMetadata) {
					t.Fatalf("interrupted Provider reasoning metadata = %s; want %s", gotMetadata, wantMetadata)
				}
			}
			if part.ToolCall != nil {
				toolCallCount++
				toolCallIndex = partIndex
				if part.ToolCall.ModelToolCallID != modelToolCallID {
					t.Fatalf("interrupted Provider Tool Call ID = %q; want %q", part.ToolCall.ModelToolCallID, modelToolCallID)
				}
			}
			if part.ToolResult != nil {
				toolResultCount++
				toolResultIndex = partIndex
				if part.ToolResult.ModelToolCallID != modelToolCallID {
					t.Fatalf("interrupted Provider Tool Result ID = %q; want %q", part.ToolResult.ModelToolCallID, modelToolCallID)
				}
			}
			partIndex++
		}
	}
	if reasoningCount != 1 || toolCallCount != 1 || toolResultCount != 1 ||
		reasoningIndex >= toolCallIndex || toolCallIndex >= toolResultIndex {
		t.Fatalf("interrupted Provider ordering = reasoning:%d@%d call:%d@%d result:%d@%d: %s",
			reasoningCount, reasoningIndex, toolCallCount, toolCallIndex, toolResultCount, toolResultIndex, raw)
	}
}

func assertInterruptedResidentContext(
	t *testing.T,
	raw json.RawMessage,
	failedText string,
	reasoningText string,
	reasoningSignature string,
	modelToolCallID string,
) {
	t.Helper()
	var entries []struct {
		Parts []struct {
			Type             string          `json:"type"`
			Text             string          `json:"text"`
			ModelToolCallID  string          `json:"modelToolCallId"`
			ProviderMetadata json.RawMessage `json:"providerMetadata"`
		} `json:"parts"`
	}
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatalf("decode interrupted resident context: %v: %s", err, raw)
	}
	reasoningIndex, toolCallIndex, toolResultIndex := -1, -1, -1
	reasoningCount, toolCallCount, toolResultCount := 0, 0, 0
	partIndex := 0
	for _, entry := range entries {
		for _, part := range entry.Parts {
			if part.Type == "text" && strings.Contains(part.Text, failedText) {
				t.Fatalf("interrupted resident context retained failed text %q: %s", failedText, raw)
			}
			if part.Type == "reasoning" {
				reasoningCount++
				reasoningIndex = partIndex
				if part.Text != reasoningText {
					t.Fatalf("interrupted resident reasoning text = %q; want %q", part.Text, reasoningText)
				}
				var metadata any
				if err := json.Unmarshal(part.ProviderMetadata, &metadata); err != nil {
					t.Fatalf("decode interrupted resident reasoning metadata: %v", err)
				}
				gotMetadata, _ := json.Marshal(metadata)
				wantMetadata, _ := json.Marshal(map[string]any{"anthropic": map[string]any{"signature": reasoningSignature}})
				if !bytes.Equal(gotMetadata, wantMetadata) {
					t.Fatalf("interrupted resident reasoning metadata = %s; want %s", gotMetadata, wantMetadata)
				}
			}
			if part.Type == "tool_call" {
				toolCallCount++
				toolCallIndex = partIndex
				if part.ModelToolCallID != modelToolCallID {
					t.Fatalf("interrupted resident Tool Call ID = %q; want %q", part.ModelToolCallID, modelToolCallID)
				}
			}
			if part.Type == "tool_result" {
				toolResultCount++
				toolResultIndex = partIndex
				if part.ModelToolCallID != modelToolCallID {
					t.Fatalf("interrupted resident Tool Result ID = %q; want %q", part.ModelToolCallID, modelToolCallID)
				}
			}
			partIndex++
		}
	}
	if reasoningCount != 1 || toolCallCount != 1 || toolResultCount != 1 ||
		reasoningIndex >= toolCallIndex || toolCallIndex >= toolResultIndex {
		t.Fatalf("interrupted resident ordering = reasoning:%d@%d call:%d@%d result:%d@%d: %s",
			reasoningCount, reasoningIndex, toolCallCount, toolCallIndex, toolResultCount, toolResultIndex, raw)
	}
}

type interruptRuntimeCompositionPaths struct {
	toolStarted        string
	operationCompleted string
	acceptResult       string
	close              string
}

func startInterruptRuntimeComposition(t *testing.T, tempDir, bridgeAddress, sessionID, threadID, bindingID string, bindingGeneration int64, podUID string, options ...interruptRuntimeCompositionOptions) (*interruptRuntimeCompositionProcess, interruptRuntimeCompositionPaths) {
	t.Helper()
	var compositionOptions interruptRuntimeCompositionOptions
	if len(options) > 0 {
		compositionOptions = options[0]
	}
	readyPath := tempDir + "/ready.json"
	paths := interruptRuntimeCompositionPaths{
		toolStarted: tempDir + "/tool-started", operationCompleted: tempDir + "/operation-completed",
		acceptResult: tempDir + "/accept-result.json", close: tempDir + "/close",
	}
	fastThreadText := compositionOptions.fastThreadText
	if fastThreadText == "" {
		fastThreadText = "post-config ordinary"
	}
	inputPath := tempDir + "/input.json"
	input, err := json.Marshal(map[string]any{
		"bridgeAddress": bridgeAddress, "workspaceId": "default", "sessionId": sessionID,
		"sessionThreadId": threadID, "bindingId": bindingID, "bindingGeneration": bindingGeneration,
		"targetPodUid": podUID, "readyPath": readyPath, "toolStartedPath": paths.toolStarted,
		"acceptResultPath":              paths.acceptResult,
		"durableOperationCompletedPath": paths.operationCompleted,
		"closePath":                     paths.close,
		"fastThreadText":                fastThreadText,
		"fastAfterFirstProviderCall":    true,
		"failFirstFinishIdle":           compositionOptions.failFirstFinishIdle,
	})
	if err != nil {
		t.Fatalf("encode interrupt Runtime composition: %v", err)
	}
	if err := os.WriteFile(inputPath, input, 0o600); err != nil {
		t.Fatalf("write interrupt Runtime composition: %v", err)
	}
	process := &interruptRuntimeCompositionProcess{}
	process.command = exec.Command("bun", "packages/runtime-pod/test/fixtures/interrupt-closeout-composition.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	process.command.Dir = "../agent-runtime"
	process.command.Stdout = &process.output
	process.command.Stderr = &process.output
	if err := process.command.Start(); err != nil {
		t.Fatalf("start interrupt Runtime composition: %v", err)
	}
	t.Cleanup(func() {
		if process.command.ProcessState == nil {
			_ = process.command.Process.Kill()
			_ = process.command.Wait()
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		raw, readErr := os.ReadFile(readyPath)
		if readErr == nil {
			var ready struct {
				Port int `json:"port"`
			}
			if json.Unmarshal(raw, &ready) == nil && ready.Port > 0 {
				process.port = ready.Port
				return process, paths
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("interrupt Runtime composition did not become ready: %s", process.output.String())
	return nil, interruptRuntimeCompositionPaths{}
}

func waitForCompositionFile(t *testing.T, path, description string, output *bytes.Buffer) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s: %s", description, output.String())
}

func (p *interruptRuntimeCompositionProcess) wait(t *testing.T) interruptRuntimeCompositionOutput {
	t.Helper()
	if err := p.command.Wait(); err != nil {
		t.Fatalf("interrupt Runtime composition: %v: %s", err, p.output.String())
	}
	var output interruptRuntimeCompositionOutput
	if err := json.Unmarshal(p.output.Bytes(), &output); err != nil {
		t.Fatalf("decode interrupt Runtime composition: %v: %s", err, p.output.String())
	}
	return output
}
