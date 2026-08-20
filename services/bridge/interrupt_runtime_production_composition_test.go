package agentruntimebridge

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
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
	InterruptResult             json.RawMessage `json:"interruptResult"`
	ProviderInvocations         int             `json:"providerInvocations"`
	DurableOperationCompletions int             `json:"durableOperationCompletions"`
}

type interruptFollowerRuntimeServer struct {
	agentruntimev1.UnimplementedAgentRuntimePodServiceServer
	calls atomic.Int32
}

type committingInterruptRuntimeSender struct {
	*recordingRuntimeCommandSender
	bridge *PostgreSQLBridgeAPIStore
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
	case 2:
		return nil, status.Error(codes.InvalidArgument, "fixture-controlled deterministic rejection")
	default:
		return &agentruntimev1.AcceptInputResponse{Outcome: &agentruntimev1.AcceptInputResponse_Rejected{Rejected: &agentruntimev1.AcceptInputRejected{
			Reason: agentruntimev1.AcceptInputFailure_ACCEPT_INPUT_FAILURE_IDENTITY_CONFLICT, Retryable: true,
		}}}, nil
	}
}

func TestPostgreSQLInterruptSettlesRetryableAndPreparedRejectionFollowers(t *testing.T) {
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

	retryID := appendMessage("interrupt-follower-retry", "retry before stop")
	if active, err := newRunner(fixturePort, "interrupt-follower-retry").RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("deliver retryable follower = active:%t err:%v", active, err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs SET available_at=clock_timestamp()+interval '1 hour'
		WHERE workspace_id='default' AND dedupe_key=$1`, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, retryID)); err != nil {
		t.Fatalf("hold retryable follower behind interrupt: %v", err)
	}
	rejectionID := appendMessage("interrupt-follower-rejection", "reject before stop")
	if active, err := newRunner(fixturePort, "interrupt-follower-rejection").RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("prepare deterministic rejection follower = active:%t err:%v", active, err)
	}
	var retryStatus, rejectionStatus, rejectionKind string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$2),
		(SELECT input_kind FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$2)`, retryID, rejectionID).
		Scan(&retryStatus, &rejectionStatus, &rejectionKind); err != nil {
		t.Fatalf("read pre-interrupt follower custody: %v", err)
	}
	if retryStatus != "delivering" || rejectionStatus != "delivering" || rejectionKind != "rejection" || followerRuntime.calls.Load() != 3 {
		t.Fatalf("pre-interrupt followers = retry:%s rejection:%s/%s calls:%d", retryStatus, rejectionStatus, rejectionKind, followerRuntime.calls.Load())
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
	var interruptInbox, interruptQueue, retryInbox, retryQueue, rejectionInbox, rejectionQueue string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$2),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$3),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$4),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$5),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$6)`,
		interruptID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, interruptID),
		retryID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, retryID),
		rejectionID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, rejectionID)).
		Scan(&interruptInbox, &interruptQueue, &retryInbox, &retryQueue, &rejectionInbox, &rejectionQueue); err != nil {
		t.Fatalf("read terminal follower custody: %v", err)
	}
	if interruptInbox != "committed" || interruptQueue != queue.StatusAcknowledged ||
		retryInbox != "cancelled" || retryQueue != queue.StatusCancelled ||
		rejectionInbox != "cancelled" || rejectionQueue != queue.StatusCancelled {
		t.Fatalf("terminal follower custody = interrupt:%s/%s retry:%s/%s rejection:%s/%s",
			interruptInbox, interruptQueue, retryInbox, retryQueue, rejectionInbox, rejectionQueue)
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
	if err := os.WriteFile(paths.close, []byte("close"), 0o600); err != nil {
		t.Fatalf("release Runtime composition: %v", err)
	}
	composed := runtimeProcess.wait(t)
	if composed.ProviderInvocations != 1 || composed.DurableOperationCompletions != 1 || len(composed.InterruptResult) == 0 {
		t.Fatalf("Runtime composition = %+v; want one Provider, one joined operation, and interrupt response", composed)
	}

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
	if inboxStatus != "committed" || queueStatus != queue.StatusAcknowledged || starts != 1 || ends != 1 || toolUses != 1 || toolResults != 1 || receipts != 1 {
		t.Fatalf("production closeout = Inbox:%s Queue:%s starts:%d ends:%d tool uses/results:%d/%d receipts:%d",
			inboxStatus, queueStatus, starts, ends, toolUses, toolResults, receipts)
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
		sessionID    = "sesn_interrupt_pod_loss_production"
		threadID     = "thr_interrupt_pod_loss_production"
		oldBindingID = "bind_interrupt_pod_loss_old"
		oldPodUID    = "pod_interrupt_pod_loss_old"
		newBindingID = "bind_interrupt_pod_loss_new"
		newPodUID    = "pod_interrupt_pod_loss_new"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, oldBindingID, 1, oldPodUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, oldBindingID, 1)
	client := dbconnect.NewClientForTesting(runtime)
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
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, newBindingID, 2, newPodUID)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_status
		SET status='running', binding_id=$2, binding_generation=2, updated_at=clock_timestamp()
		WHERE workspace_id='default' AND session_id=$1`, sessionID, newBindingID); err != nil {
		t.Fatalf("install replacement Runtime status fence: %v", err)
	}

	bridgeStore := NewPostgreSQLBridgeAPIStore(client)
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
	runtimeProcess, paths := startInterruptRuntimeComposition(t, t.TempDir(), bridgeListener.Addr().String(), sessionID, threadID, newBindingID, 2, newPodUID)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_bindings SET agent_runtime_pod_ip='127.0.0.1'
		WHERE workspace_id='default' AND session_id=$1 AND binding_id=$2`, sessionID, newBindingID); err != nil {
		t.Fatalf("align replacement Runtime binding: %v", err)
	}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(client, runtimeProcess.port)
	deliveryStore.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: newPodUID, PodIP: "127.0.0.1",
		}})
	}}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queue.NewPostgreSQLStore(client), nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: NewRuntimePodCommandClient(taskNotificationRuntimeTokenSource{})},
		Config:    JobRunnerConfig{LeaseOwner: "interrupt-pod-loss-replacement", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("deliver same interrupt after pod loss = active:%t err:%v", active, err)
	}
	if err := os.WriteFile(paths.close, []byte("close"), 0o600); err != nil {
		t.Fatalf("release pod-loss Runtime composition: %v", err)
	}
	composed := runtimeProcess.wait(t)
	if len(composed.InterruptResult) == 0 || composed.ProviderInvocations != 0 {
		t.Fatalf("pod-loss interrupt Runtime composition = %+v", composed)
	}
	var finalInbox, finalQueue string
	var attemptsAfter, lineage, receipts, activeBarriers int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$3),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$4
		 AND source_kind='interrupt_control' AND idempotency_key=$1 AND receipt_json <> ''),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$4
		 AND input_kind='interrupt_control' AND status IN ('queued','delivering','accepted'))`,
		interruptID, jobID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, interruptID), sessionID).
		Scan(&finalInbox, &finalQueue, &attemptsAfter, &lineage, &receipts, &activeBarriers); err != nil {
		t.Fatalf("read pod-loss interrupt terminal facts: %v", err)
	}
	if finalInbox != "committed" || finalQueue != queue.StatusAcknowledged || attemptsAfter != attemptsBefore+1 || lineage != 1 || receipts != 1 || activeBarriers != 0 {
		t.Fatalf("pod-loss terminal facts = Inbox:%s Queue:%s attempts:%d->%d lineage:%d receipts:%d barriers:%d",
			finalInbox, finalQueue, attemptsBefore, attemptsAfter, lineage, receipts, activeBarriers)
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
	}); !isSessionInterruptBarrierStaleError(err) {
		t.Fatalf("interrupted-source mail = %v; want barrier stale", err)
	}
	if _, err := bridgeStore.CreateSubagentThread(context.Background(), &bridgev1.CreateSubagentThreadRequest{
		Scope: siblingScope, SourceToolUseEventId: lateChild, TaskName: "late-child", AgentType: "worker", ForkTurns: "all",
	}); !isSessionInterruptBarrierStaleError(err) {
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
		mailInbox != "committed" || mailQueue != queue.StatusAcknowledged || lateOperations != 0 || lateChildren != 0 || activeBarriers != 0 {
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

type interruptRuntimeCompositionPaths struct {
	toolStarted        string
	operationCompleted string
	acceptResult       string
	close              string
}

func startInterruptRuntimeComposition(t *testing.T, tempDir, bridgeAddress, sessionID, threadID, bindingID string, bindingGeneration int64, podUID string) (*interruptRuntimeCompositionProcess, interruptRuntimeCompositionPaths) {
	t.Helper()
	readyPath := tempDir + "/ready.json"
	paths := interruptRuntimeCompositionPaths{
		toolStarted: tempDir + "/tool-started", operationCompleted: tempDir + "/operation-completed",
		acceptResult: tempDir + "/accept-result.json", close: tempDir + "/close",
	}
	inputPath := tempDir + "/input.json"
	input, err := json.Marshal(map[string]any{
		"bridgeAddress": bridgeAddress, "workspaceId": "default", "sessionId": sessionID,
		"sessionThreadId": threadID, "bindingId": bindingID, "bindingGeneration": bindingGeneration,
		"targetPodUid": podUID, "readyPath": readyPath, "toolStartedPath": paths.toolStarted,
		"acceptResultPath":              paths.acceptResult,
		"durableOperationCompletedPath": paths.operationCompleted, "closePath": paths.close,
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
