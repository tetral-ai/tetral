package agentruntimebridge

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
)

func TestJobRunnerRevalidatesPreparedInterruptAfterConcurrentRequestEnd(t *testing.T) {
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
	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
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
	if len(sender.requests) != 0 {
		t.Fatalf("stale prepared interrupt reached successor Runtime = %#v", sender.requests)
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

type preparedInterruptBarrierStore struct {
	*PostgreSQLRuntimeDeliveryStore
	prepared chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (s *preparedInterruptBarrierStore) PrepareRuntimeCommand(ctx context.Context, job RuntimeJob) (RuntimeCommandPlan, error) {
	plan, err := s.PostgreSQLRuntimeDeliveryStore.PrepareRuntimeCommand(ctx, job)
	if err == nil && plan.Interrupt != nil {
		s.once.Do(func() {
			close(s.prepared)
			select {
			case <-s.release:
			case <-ctx.Done():
			}
		})
	}
	return plan, err
}

func (s *preparedInterruptBarrierStore) RepairLostRuntimeBindings(context.Context, string) (int, error) {
	return 0, nil
}

var _ RuntimeDeliveryStore = (*preparedInterruptBarrierStore)(nil)
var _ RuntimeDeliveryFinalizationStore = (*preparedInterruptBarrierStore)(nil)
var _ RuntimePodLossRepairer = (*preparedInterruptBarrierStore)(nil)
