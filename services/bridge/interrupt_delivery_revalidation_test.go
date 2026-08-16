package agentruntimebridge

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
)

func TestPostgreSQLRuntimeDeliveryStoreKeepsBindingChangeRetryableDuringInterruptRevalidation(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID   = "sesn_interrupt_binding_change"
		threadID    = "thr_interrupt_binding_change"
		interruptID = "rin_interrupt_binding_change"
		eventID     = "sevt_interrupt_binding_change"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_interrupt_old", 1, "pod_interrupt_old")
	seedBridgeAPIOpenDurableTurn(t, admin, bridgeAPIScope(sessionID, threadID, "bind_interrupt_old", 1, "pod_interrupt_old"), "evt_interrupt_binding_run")
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 2, "user.interrupt", `{}`)
	job := RuntimeJob{
		JobID: "qjob_interrupt_binding_change", LeaseToken: "lease_interrupt_binding_change", Kind: queue.KindRuntimeInput,
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: interruptID, EventIDs: []string{eventID}, SequenceFrom: 2, SequenceTo: 2, InputKind: "interrupt_control",
		PayloadJSON: `{"workspace_id":"default","session_id":"sesn_interrupt_binding_change","session_thread_id":"thr_interrupt_binding_change","runtime_input_id":"rin_interrupt_binding_change","event_ids":["sevt_interrupt_binding_change"],"sequence_from":2,"sequence_to":2,"input_kind":"interrupt_control"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, job)
	resolver := &recordingRuntimeTargetResolver{binding: runtimeBindingForDelivery{
		BindingID: "bind_interrupt_old", BindingGeneration: 1, Namespace: "engine", PodName: "runtime-old",
		PodUID: "pod_interrupt_old", PodIP: "127.0.0.1",
	}}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.TargetResolver = resolver
	if first, err := store.PrepareRuntimeCommand(context.Background(), job); err != nil || first.Interrupt == nil {
		t.Fatalf("first interrupt prepare = %#v/%v; want old-binding command", first, err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_bindings
		SET binding_id='bind_interrupt_new', binding_generation=2, agent_runtime_pod_uid='pod_interrupt_new'
		WHERE workspace_id='default' AND session_id=$1`, sessionID); err != nil {
		t.Fatalf("replace interrupt binding: %v", err)
	}
	resolver.binding = runtimeBindingForDelivery{
		BindingID: "bind_interrupt_new", BindingGeneration: 2, Namespace: "engine", PodName: "runtime-new",
		PodUID: "pod_interrupt_new", PodIP: "127.0.0.2",
	}
	_, err := store.PrepareRuntimeCommand(context.Background(), job)
	var prepareErr runtimeDeliveryPrepareError
	if !errors.As(err, &prepareErr) || !prepareErr.retryable || prepareErr.kind != "runtime_inbox_binding_changed" {
		t.Fatalf("binding-change revalidation err = %v; want retryable runtime_inbox_binding_changed", err)
	}
	var statusValue, bindingID, podUID string
	var generation int64
	if err := admin.QueryRowContext(context.Background(), `SELECT status,binding_id,binding_generation,target_pod_uid
		FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1`, interruptID).
		Scan(&statusValue, &bindingID, &generation, &podUID); err != nil {
		t.Fatalf("read preserved interrupt custody: %v", err)
	}
	if statusValue != "delivering" || bindingID != "bind_interrupt_old" || generation != 1 || podUID != "pod_interrupt_old" {
		t.Fatalf("interrupt custody = %s/%s/%d/%s; want unchanged old attempt", statusValue, bindingID, generation, podUID)
	}
}

func TestJobRunnerRuntimeFenceRejectsInterruptSettledAfterFinalBridgeCheck(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID        = "sesn_interrupt_revalidation"
		threadID         = "thr_interrupt_revalidation"
		bindingID        = "bind_interrupt_revalidation"
		podUID           = "pod_interrupt_revalidation"
		interruptID      = "rin_interrupt_revalidation"
		interruptEventID = "sevt_interrupt_revalidation"
		modelRequestID   = "mreq_interrupt_revalidation"
	)
	now := time.Now().UTC()
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	apiStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIOpenDurableTurn(t, admin, scope, "rwrite_interrupt_revalidation_run")
	seedBridgeAPIRequestStart(
		t,
		apiStore,
		scope,
		"rwrite_interrupt_revalidation_start",
		modelRequestID,
		requestKindAgentProviderRequest,
		0,
	)
	var interruptSequence int64
	if err := admin.QueryRowContext(context.Background(), `SELECT COALESCE(MAX(sequence), 0) + 1
		FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2`, sessionID, threadID).
		Scan(&interruptSequence); err != nil {
		t.Fatalf("allocate interrupt sequence: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, interruptEventID, interruptSequence, "user.interrupt", `{"type":"user.interrupt"}`)

	payload, err := json.Marshal(map[string]any{
		"workspace_id": workspace.DefaultID, "session_id": sessionID, "session_thread_id": threadID,
		"runtime_input_id": interruptID, "event_ids": []string{interruptEventID},
		"sequence_from": interruptSequence, "sequence_to": interruptSequence, "input_kind": "interrupt_control",
	})
	if err != nil {
		t.Fatalf("encode interrupt Queue payload: %v", err)
	}
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: interruptID, InputKind: "interrupt_control", EventIDs: []string{interruptEventID},
		SequenceFrom: interruptSequence, SequenceTo: interruptSequence,
	})
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	queued, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
		ID: "qjob_intr_revalidate", WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, interruptID),
		PayloadVersion: 1, PayloadJSON: payload, MaxAttempts: queue.DefaultMaxAttempts,
		AvailableAt: now, Now: now,
	})
	if err != nil {
		t.Fatalf("enqueue interrupt: %v", err)
	}

	productionStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	productionStore.TargetResolver = &recordingRuntimeTargetResolver{binding: runtimeBindingForDelivery{
		BindingID: bindingID, BindingGeneration: 1, Namespace: "engine", PodName: "runtime-interrupt-revalidation",
		PodUID: podUID, PodIP: "127.0.0.1",
	}}
	barrierStore := &preparedInterruptBarrierStore{
		PostgreSQLRuntimeDeliveryStore: productionStore,
		prepared:                       make(chan struct{}),
		release:                        make(chan struct{}),
	}
	sender := &durableInterruptFenceSender{
		recordingRuntimeCommandSender: &recordingRuntimeCommandSender{},
		store:                         apiStore,
	}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: RuntimePodDirectDeliverer{Store: barrierStore, Sender: sender},
		Config: JobRunnerConfig{
			LeaseOwner: "interrupt-revalidation", MaxJobs: 1,
			LeaseDuration: 120 * time.Millisecond, HeartbeatInterval: 15 * time.Millisecond,
		},
	}
	done := make(chan error, 1)
	go func() { done <- runner.RunOnce(context.Background()) }()
	select {
	case <-barrierStore.prepared:
	case <-time.After(5 * time.Second):
		t.Fatal("interrupt attempt did not reach the post-prepare barrier")
	}

	var initialLease time.Time
	if err := admin.QueryRowContext(context.Background(), `SELECT leased_until FROM queue_jobs WHERE workspace_id='default' AND id=$1`, queued.ID).
		Scan(&initialLease); err != nil {
		t.Fatalf("read initial Queue lease: %v", err)
	}
	heartbeatObserved := false
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		var currentLease time.Time
		if err := admin.QueryRowContext(context.Background(), `SELECT leased_until FROM queue_jobs WHERE workspace_id='default' AND id=$1`, queued.ID).
			Scan(&currentLease); err != nil {
			t.Fatalf("read heartbeat Queue lease: %v", err)
		}
		if currentLease.After(initialLease) {
			heartbeatObserved = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !heartbeatObserved {
		t.Fatal("live Queue lease was not extended while the prepared interrupt was blocked")
	}

	ended, err := apiStore.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_interrupt_revalidation_end", ModelRequestId: modelRequestID,
		FinishReason: "error", UsageJson: `{}`, IsError: true, ErrorKind: "runtime_interrupted",
		InterruptSettlement: &bridgev1.RequestEndInterruptSettlement{RuntimeInputId: interruptID},
	})
	if err != nil || ended.GetCommitted() == nil {
		t.Fatalf("settle interrupt through Request End = %#v/%v", ended, err)
	}
	successor, err := apiStore.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_interrupt_revalidation_successor",
		EventType: "session.status_running", PayloadJson: `{"type":"session.status_running"}`,
	})
	if err != nil || successor.GetCommitted() == nil {
		t.Fatalf("open successor run = %#v/%v", successor, err)
	}

	close(barrierStore.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run interrupt Queue delivery: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("interrupt Queue delivery did not finish")
	}
	if len(sender.requests) != 0 || sender.stale {
		t.Fatalf("Runtime fence = requests %#v stale %v; want pre-send revalidation to suppress transport", sender.requests, sender.stale)
	}
	var threadStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_threads
		WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, threadID).Scan(&threadStatus); err != nil {
		t.Fatalf("read successor thread status: %v", err)
	}
	if threadStatus != "running" {
		t.Fatalf("successor thread status = %q; want running after stale interrupt delivery", threadStatus)
	}
	var queueStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$1`, queued.ID).
		Scan(&queueStatus); err != nil {
		t.Fatalf("read final Queue status: %v", err)
	}
	if queueStatus != queue.StatusAcknowledged {
		t.Fatalf("stale interrupt Queue status = %s; want acknowledged duplicate", queueStatus)
	}
}

func TestRuntimeFinalCommitFenceRejectsSettlementAfterPreSendRevalidation(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID        = "sesn_interrupt_final_fence"
		threadID         = "thr_interrupt_final_fence"
		bindingID        = "bind_interrupt_final_fence"
		podUID           = "pod_interrupt_final_fence"
		interruptID      = "rin_interrupt_final_fence"
		interruptEventID = "sevt_interrupt_final_fence"
		modelRequestID   = "mreq_interrupt_final_fence"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	apiStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIOpenDurableTurn(t, admin, scope, "rwrite_interrupt_final_fence_run")
	seedBridgeAPIRequestStart(t, apiStore, scope, "rwrite_interrupt_final_fence_start", modelRequestID, requestKindAgentProviderRequest, 0)
	var interruptSequence int64
	if err := admin.QueryRowContext(context.Background(), `SELECT COALESCE(MAX(sequence), 0) + 1
		FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2`, sessionID, threadID).
		Scan(&interruptSequence); err != nil {
		t.Fatalf("allocate final-fence interrupt sequence: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, interruptEventID, interruptSequence, "user.interrupt", `{}`)
	job := RuntimeJob{
		JobID: "qjob_interrupt_final_fence", LeaseToken: "lease_interrupt_final_fence", Kind: queue.KindRuntimeInput,
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: interruptID, EventIDs: []string{interruptEventID}, SequenceFrom: interruptSequence,
		SequenceTo: interruptSequence, InputKind: "interrupt_control",
	}
	payloadJSON, err := json.Marshal(map[string]any{
		"workspace_id": "default", "session_id": sessionID, "session_thread_id": threadID,
		"runtime_input_id": interruptID, "event_ids": []string{interruptEventID},
		"sequence_from": interruptSequence, "sequence_to": interruptSequence, "input_kind": "interrupt_control",
	})
	if err != nil {
		t.Fatalf("encode final-fence interrupt payload: %v", err)
	}
	job.PayloadJSON = string(payloadJSON)
	seedRuntimeInboxBirthForJob(t, admin, job)
	productionStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	productionStore.TargetResolver = &recordingRuntimeTargetResolver{binding: runtimeBindingForDelivery{
		BindingID: bindingID, BindingGeneration: 1, Namespace: "engine", PodName: "runtime-interrupt-final-fence",
		PodUID: podUID, PodIP: "127.0.0.1",
	}}
	barrierStore := &postPreparedInterruptBarrierStore{
		PostgreSQLRuntimeDeliveryStore: productionStore,
		prepared:                       make(chan struct{}),
		release:                        make(chan struct{}),
	}
	sender := &durableInterruptFenceSender{
		recordingRuntimeCommandSender: &recordingRuntimeCommandSender{},
		store:                         apiStore,
	}
	done := make(chan struct {
		result RuntimeDeliveryResult
		err    error
	}, 1)
	go func() {
		result, err := (RuntimePodDirectDeliverer{Store: barrierStore, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
		done <- struct {
			result RuntimeDeliveryResult
			err    error
		}{result: result, err: err}
	}()
	select {
	case <-barrierStore.prepared:
	case <-time.After(5 * time.Second):
		t.Fatal("interrupt did not reach the post-revalidation barrier")
	}
	ended, err := apiStore.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_interrupt_final_fence_end", ModelRequestId: modelRequestID,
		FinishReason: "error", UsageJson: `{}`, IsError: true, ErrorKind: "runtime_interrupted",
		InterruptSettlement: &bridgev1.RequestEndInterruptSettlement{RuntimeInputId: interruptID},
	})
	if err != nil || ended.GetCommitted() == nil {
		t.Fatalf("settle final-fence interrupt = %#v/%v", ended, err)
	}
	if successor, err := apiStore.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_interrupt_final_fence_successor",
		EventType: "session.status_running", PayloadJson: `{"type":"session.status_running"}`,
	}); err != nil || successor.GetCommitted() == nil {
		t.Fatalf("open final-fence successor = %#v/%v", successor, err)
	}
	close(barrierStore.release)
	select {
	case outcome := <-done:
		if outcome.err != nil || outcome.result.Status != RuntimeDeliveryDuplicate {
			t.Fatalf("final-fence delivery = %#v/%v; want exact duplicate", outcome.result, outcome.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("final-fence delivery did not finish")
	}
	if len(sender.requests) != 1 || sender.stale {
		t.Fatalf("final Runtime fence = requests %#v stale %v; want one sent command joined to the exact committed receipt", sender.requests, sender.stale)
	}
}

type preparedInterruptBarrierStore struct {
	*PostgreSQLRuntimeDeliveryStore
	prepared chan struct{}
	release  chan struct{}
	mu       sync.Mutex
	calls    int
}

type postPreparedInterruptBarrierStore struct {
	*PostgreSQLRuntimeDeliveryStore
	prepared chan struct{}
	release  chan struct{}
	mu       sync.Mutex
	calls    int
}

func (s *postPreparedInterruptBarrierStore) PrepareRuntimeCommand(ctx context.Context, job RuntimeJob) (RuntimeCommandPlan, error) {
	plan, err := s.PostgreSQLRuntimeDeliveryStore.PrepareRuntimeCommand(ctx, job)
	if err == nil && plan.Interrupt != nil {
		s.mu.Lock()
		s.calls++
		finalCheck := s.calls == 2
		s.mu.Unlock()
		if finalCheck {
			close(s.prepared)
			select {
			case <-s.release:
			case <-ctx.Done():
			}
		}
	}
	return plan, err
}

func (s *postPreparedInterruptBarrierStore) RepairLostRuntimeBindings(context.Context, string) (int, error) {
	return 0, nil
}

var _ RuntimeDeliveryStore = (*postPreparedInterruptBarrierStore)(nil)

func (s *preparedInterruptBarrierStore) PrepareRuntimeCommand(ctx context.Context, job RuntimeJob) (RuntimeCommandPlan, error) {
	s.mu.Lock()
	s.calls++
	finalCheck := s.calls == 2
	s.mu.Unlock()
	if finalCheck {
		close(s.prepared)
		select {
		case <-s.release:
		case <-ctx.Done():
		}
	}
	return s.PostgreSQLRuntimeDeliveryStore.PrepareRuntimeCommand(ctx, job)
}

func (s *preparedInterruptBarrierStore) RepairLostRuntimeBindings(context.Context, string) (int, error) {
	return 0, nil
}

var _ RuntimeDeliveryStore = (*preparedInterruptBarrierStore)(nil)
var _ RuntimeDeliveryFinalizationStore = (*preparedInterruptBarrierStore)(nil)
var _ RuntimePodLossRepairer = (*preparedInterruptBarrierStore)(nil)

type durableInterruptFenceSender struct {
	*recordingRuntimeCommandSender
	store *PostgreSQLBridgeAPIStore
	stale bool
}

func (s *durableInterruptFenceSender) Interrupt(
	ctx context.Context,
	target RuntimePodTarget,
	request *agentruntimev1.InterruptRequest,
) (*agentruntimev1.InterruptResponse, error) {
	s.targets = append(s.targets, target)
	s.requests = append(s.requests, request)
	committed, err := s.store.CommitInputs(ctx, &bridgev1.CommitInputsRequest{
		Scope: bridgeAPIScope(
			request.GetSessionId(),
			request.GetSessionThreadId(),
			request.GetBindingId(),
			request.GetBindingGeneration(),
			request.GetTargetPodUid(),
		),
		RuntimeInputId: request.GetRuntimeInputId(),
	})
	if err != nil {
		return nil, err
	}
	if committed.GetCommitted() == nil && committed.GetStale() == nil {
		return nil, status.Error(codes.Internal, "interrupt durable fence returned no closed result")
	}
	s.stale = committed.GetStale() != nil
	return &agentruntimev1.InterruptResponse{
		Outcome: &agentruntimev1.InterruptResponse_Duplicate{
			Duplicate: &agentruntimev1.InterruptDuplicate{},
		},
	}, nil
}

var _ RuntimeCommandSender = (*durableInterruptFenceSender)(nil)
