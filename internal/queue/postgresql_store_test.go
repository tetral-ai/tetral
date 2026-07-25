package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestPostgreSQLStoreLeasePriorityPartitionBarrierAndAckFence(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	ws := workspace.ID("ws_queue_barrier")
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	sessionID := "sesn_queue_a"
	otherSessionID := "sesn_queue_b"
	partition := FormatSessionPartitionKey(ws, sessionID)
	otherPartition := FormatSessionPartitionKey(ws, otherSessionID)

	mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_low",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, "input_low"),
		PayloadJSON:  runtimeInputPayload(t, ws, sessionID, "thrd_queue_a", "input_low", "messages", 1, 1),
		Priority:     0,
		Now:          now,
	})
	interrupt := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_high",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, "input_interrupt"),
		PayloadJSON:  runtimeInputPayload(t, ws, sessionID, "thrd_queue_a", "input_interrupt", "interrupt_control", 2, 2),
		Priority:     100,
		Now:          now.Add(time.Millisecond),
	})
	other := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_other",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: otherPartition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, otherSessionID, "input_other"),
		PayloadJSON:  runtimeInputPayload(t, ws, otherSessionID, "thrd_queue_b", "input_other", "messages", 1, 1),
		Priority:     0,
		Now:          now.Add(2 * time.Millisecond),
	})

	leased, err := store.Lease(ctx, LeaseRequest{
		WorkspaceID:   ws,
		Kinds:         []string{KindRuntimeInput},
		LeaseOwner:    "bridge-job-runner",
		MaxJobs:       2,
		LeaseDuration: time.Minute,
		Now:           now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	assertLeasedIDs(t, leased, []string{interrupt.ID, other.ID})
	if got := queueJobStatus(t, admin, ws, "qjob_low"); got != StatusPending {
		t.Fatalf("low-priority same-partition job status = %s; want pending barrier", got)
	}
	if ok, err := store.Ack(ctx, AckRequest{WorkspaceID: ws, JobID: interrupt.ID, LeaseToken: "stale-token", Now: now.Add(2 * time.Second)}); err != nil || ok {
		t.Fatalf("stale Ack = (%v,%v); want false,nil", ok, err)
	}
	if got := queueJobStatus(t, admin, ws, interrupt.ID); got != StatusLeased {
		t.Fatalf("stale Ack changed status to %s; want leased", got)
	}
	if ok, err := store.Ack(ctx, AckRequest{WorkspaceID: ws, JobID: interrupt.ID, LeaseToken: leased[0].LeaseToken, Now: now.Add(3 * time.Second)}); err != nil || !ok {
		t.Fatalf("Ack = (%v,%v); want true,nil", ok, err)
	}
	next, err := store.Lease(ctx, LeaseRequest{
		WorkspaceID:   ws,
		Kinds:         []string{KindRuntimeInput},
		LeaseOwner:    "bridge-job-runner",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(4 * time.Second),
	})
	if err != nil {
		t.Fatalf("second Lease: %v", err)
	}
	assertLeasedIDs(t, next, []string{"qjob_low"})
}

func TestPostgreSQLStoreLeaseHonorsCrossKindSessionBarrier(t *testing.T) {
	store, _ := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	ws := workspace.ID("ws_queue_cross_kind")
	sessionID := "sesn_cross_kind"
	now := time.Date(2026, 7, 1, 12, 30, 0, 0, time.UTC)
	partition := FormatSessionPartitionKey(ws, sessionID)

	message := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_cross_message",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, "input_message"),
		PayloadJSON:  runtimeInputPayload(t, ws, sessionID, "thrd_cross_kind", "input_message", "messages", 1, 1),
		Now:          now,
	})
	config := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_cross_config",
		WorkspaceID:  ws,
		Kind:         KindRuntimeConfigUpdate,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeConfigUpdateDedupeKey(ws, sessionID, "42"),
		PayloadJSON:  []byte(`{"workspace_id":"ws_queue_cross_kind","session_id":"sesn_cross_kind","config_generation":42}`),
		Now:          now.Add(time.Millisecond),
	})
	interrupt := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_cross_interrupt",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, "input_interrupt"),
		PayloadJSON:  runtimeInputPayload(t, ws, sessionID, "thrd_cross_kind", "input_interrupt", "interrupt_control", 2, 2),
		Priority:     100,
		Now:          now.Add(2 * time.Millisecond),
	})

	first := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID:   ws,
		Kinds:         []string{KindRuntimeInput, KindRuntimeConfigUpdate},
		LeaseOwner:    "bridge",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(time.Second),
	})
	if first.ID != message.ID {
		t.Fatalf("first cross-kind lease = %s; want earlier message before config/interrupt", first.ID)
	}
	if ok, err := store.Ack(ctx, AckRequest{WorkspaceID: ws, JobID: first.ID, LeaseToken: first.LeaseToken, Now: now.Add(2 * time.Second)}); err != nil || !ok {
		t.Fatalf("Ack first cross-kind job = (%v,%v); want true,nil", ok, err)
	}
	second := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID:   ws,
		Kinds:         []string{KindRuntimeInput, KindRuntimeConfigUpdate},
		LeaseOwner:    "bridge",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(3 * time.Second),
	})
	if second.ID != config.ID {
		t.Fatalf("second cross-kind lease = %s; want pending config barrier before interrupt %s", second.ID, interrupt.ID)
	}
	if ok, err := store.Ack(ctx, AckRequest{WorkspaceID: ws, JobID: second.ID, LeaseToken: second.LeaseToken, Now: now.Add(4 * time.Second)}); err != nil || !ok {
		t.Fatalf("Ack second cross-kind job = (%v,%v); want true,nil", ok, err)
	}
	third := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID:   ws,
		Kinds:         []string{KindRuntimeInput, KindRuntimeConfigUpdate},
		LeaseOwner:    "bridge",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(5 * time.Second),
	})
	if third.ID != interrupt.ID {
		t.Fatalf("third cross-kind lease = %s; want interrupt after config barrier", third.ID)
	}
}

func TestPostgreSQLStoreLeaseRuntimeConfigBeforeRetriedRuntimeInput(t *testing.T) {
	store, _ := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	ws := workspace.ID("ws_queue_manifest_retry")
	sessionID := "sesn_manifest_retry"
	now := time.Date(2026, 7, 1, 12, 40, 0, 0, time.UTC)
	partition := FormatSessionPartitionKey(ws, sessionID)
	message := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_mretry_input",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, "input_message"),
		PayloadJSON:  runtimeInputPayload(t, ws, sessionID, "thrd_manifest_retry", "input_message", "messages", 1, 1),
		Now:          now,
	})
	leasedMessage := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID:   ws,
		Kinds:         []string{KindRuntimeInput, KindRuntimeConfigUpdate},
		LeaseOwner:    "bridge",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(time.Second),
	})
	if leasedMessage.ID != message.ID {
		t.Fatalf("first lease = %s; want message %s", leasedMessage.ID, message.ID)
	}
	manifest := mustEnqueue(t, store, EnqueueRequest{
		ID:             "qjob_mretry_config",
		WorkspaceID:    ws,
		Kind:           KindRuntimeConfigUpdate,
		PartitionKey:   partition,
		DedupeKey:      FormatRuntimeMCPManifestUpdateDedupeKey(ws, sessionID, "github", "1"),
		PayloadVersion: 2,
		PayloadJSON: queuePayload(t, map[string]any{
			"workspace_id":        string(ws),
			"session_id":          sessionID,
			"mcp_server_name":     "github",
			"manifest_generation": 1,
		}),
		Now: now.Add(2 * time.Second),
	})
	if ok, err := store.Retry(ctx, RetryRequest{
		WorkspaceID:  ws,
		JobID:        leasedMessage.ID,
		LeaseToken:   leasedMessage.LeaseToken,
		ErrorKind:    "mcp_manifest_discovery_pending",
		ErrorMessage: "mcp manifest update queued before runtime input delivery",
		Now:          now.Add(2 * time.Second),
	}); err != nil || !ok {
		t.Fatalf("Retry message after manifest enqueue = (%v,%v); want true,nil", ok, err)
	}
	next := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID:   ws,
		Kinds:         []string{KindRuntimeInput, KindRuntimeConfigUpdate},
		LeaseOwner:    "bridge",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(3 * time.Second),
	})
	if next.ID != manifest.ID {
		t.Fatalf("next lease = %s; want manifest config %s before retried message", next.ID, manifest.ID)
	}
}

func TestPostgreSQLStoreLeaseCandidateWindowCannotStarveEarlierBarrierJob(t *testing.T) {
	store, _ := newPostgreSQLQueueStore(t)
	ws := workspace.ID("ws_queue_starve")
	sessionID := "sesn_starve"
	now := time.Date(2026, 7, 1, 12, 45, 0, 0, time.UTC)
	partition := FormatSessionPartitionKey(ws, sessionID)
	config := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_starve_config",
		WorkspaceID:  ws,
		Kind:         KindRuntimeConfigUpdate,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeConfigUpdateDedupeKey(ws, sessionID, "7"),
		PayloadJSON:  []byte(`{"workspace_id":"ws_queue_starve","session_id":"sesn_starve","config_generation":7}`),
		Now:          now,
	})
	for index := 0; index < 24; index++ {
		inputID := "input_interrupt_" + string(rune('a'+index))
		mustEnqueue(t, store, EnqueueRequest{
			ID:           "qjob_starve_int_" + string(rune('a'+index)),
			WorkspaceID:  ws,
			Kind:         KindRuntimeInput,
			PartitionKey: partition,
			DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, inputID),
			PayloadJSON:  runtimeInputPayload(t, ws, sessionID, "thrd_starve", inputID, "interrupt_control", int64(index+1), int64(index+1)),
			Priority:     100,
			Now:          now.Add(time.Duration(index+1) * time.Millisecond),
		})
	}
	leased := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID:   ws,
		Kinds:         []string{KindRuntimeInput, KindRuntimeConfigUpdate},
		LeaseOwner:    "bridge",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(time.Second),
	})
	if leased.ID != config.ID {
		t.Fatalf("starved lease = %s; want earlier config barrier %s", leased.ID, config.ID)
	}
}

func TestPostgreSQLStoreActiveDedupeReplay(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	ws := workspace.ID("ws_queue_dedupe")
	now := time.Date(2026, 7, 1, 13, 0, 0, 0, time.UTC)
	sessionID := "sesn_queue"
	dedupe := FormatSessionPrepareDedupeKey(ws, sessionID, "prep_1")

	first := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_prepare_first",
		WorkspaceID:  ws,
		Kind:         KindSessionPrepare,
		PartitionKey: FormatSessionPartitionKey(ws, sessionID),
		DedupeKey:    dedupe,
		PayloadJSON:  []byte(`{"workspace_id":"ws_queue_dedupe","session_id":"sesn_queue","preparation_attempt_id":"prep_1"}`),
		Now:          now,
	})
	replay := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_prepare_replay",
		WorkspaceID:  ws,
		Kind:         KindSessionPrepare,
		PartitionKey: FormatSessionPartitionKey(ws, sessionID),
		DedupeKey:    dedupe,
		PayloadJSON:  []byte(`{"workspace_id":"ws_queue_dedupe","session_id":"sesn_queue","preparation_attempt_id":"prep_1"}`),
		Now:          now.Add(time.Second),
	})
	if replay.ID != first.ID {
		t.Fatalf("dedupe replay ID = %s; want existing %s", replay.ID, first.ID)
	}
	if got := countQueueJobsByDedupe(t, admin, ws, dedupe); got != 1 {
		t.Fatalf("active dedupe job count = %d; want 1", got)
	}
	leased, err := store.Lease(ctx, LeaseRequest{
		WorkspaceID:   ws,
		Kinds:         []string{KindSessionPrepare},
		LeaseOwner:    "sandbox-service",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	assertLeasedIDs(t, leased, []string{first.ID})
	if ok, err := store.Ack(ctx, AckRequest{WorkspaceID: ws, JobID: first.ID, LeaseToken: leased[0].LeaseToken, Now: now.Add(4 * time.Second)}); err != nil || !ok {
		t.Fatalf("Ack = (%v,%v); want true,nil", ok, err)
	}
	second := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_prepare_second",
		WorkspaceID:  ws,
		Kind:         KindSessionPrepare,
		PartitionKey: FormatSessionPartitionKey(ws, sessionID),
		DedupeKey:    dedupe,
		PayloadJSON:  []byte(`{"workspace_id":"ws_queue_dedupe","session_id":"sesn_queue","preparation_attempt_id":"prep_1"}`),
		Now:          now.Add(5 * time.Second),
	})
	if second.ID == first.ID {
		t.Fatal("dedupe replay reused acknowledged job; active dedupe must exclude terminal jobs")
	}
}

func TestPostgreSQLStoreCancelInterruptFenceOnlyCancelsPendingOlderSameThreadMessages(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	ws := workspace.ID("ws_queue_cancel")
	sessionID := "sesn_cancel"
	threadID := "thrd_cancel"
	otherThreadID := "thrd_other"
	now := time.Date(2026, 7, 1, 13, 30, 0, 0, time.UTC)
	partition := FormatSessionPartitionKey(ws, sessionID)

	leasedOld := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_cancel_lease",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, "input_leased_old"),
		PayloadJSON:  runtimeInputPayload(t, ws, sessionID, threadID, "input_leased_old", "messages", 1, 1),
		Now:          now,
	})
	pendingOld := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_cancel_pend",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, "input_pending_old"),
		PayloadJSON:  runtimeInputPayload(t, ws, sessionID, threadID, "input_pending_old", "messages", 2, 4),
		Now:          now.Add(time.Millisecond),
	})
	equalFence := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_cancel_equal",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, "input_equal_fence"),
		PayloadJSON:  runtimeInputPayload(t, ws, sessionID, threadID, "input_equal_fence", "messages", 5, 5),
		Now:          now.Add(2 * time.Millisecond),
	})
	otherThread := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_cancel_other",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, "input_other_thread"),
		PayloadJSON:  runtimeInputPayload(t, ws, sessionID, otherThreadID, "input_other_thread", "messages", 2, 4),
		Now:          now.Add(3 * time.Millisecond),
	})
	toolConfirmation := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_cancel_tool",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, "input_tool_confirmation"),
		PayloadJSON:  runtimeInputPayload(t, ws, sessionID, threadID, "input_tool_confirmation", "tool_confirmation", 2, 4),
		Now:          now.Add(4 * time.Millisecond),
	})
	leased := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID:   ws,
		Kinds:         []string{KindRuntimeInput},
		LeaseOwner:    "bridge",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(time.Second),
	})
	if leased.ID != leasedOld.ID {
		t.Fatalf("leased old job = %s; want %s", leased.ID, leasedOld.ID)
	}

	cancelled, err := store.Cancel(ctx, CancelRequest{
		WorkspaceID:            ws,
		SessionID:              sessionID,
		SessionThreadID:        threadID,
		InterruptFenceSequence: 5,
		Now:                    now.Add(2 * time.Second),
	})
	if err != nil || cancelled != 1 {
		t.Fatalf("Cancel interrupt fence = (%d,%v); want 1,nil", cancelled, err)
	}
	if got := queueJobStatus(t, admin, ws, leasedOld.ID); got != StatusLeased {
		t.Fatalf("leased old status = %s; want leased", got)
	}
	if got := queueJobStatus(t, admin, ws, pendingOld.ID); got != StatusCancelled {
		t.Fatalf("pending old status = %s; want cancelled", got)
	}
	for _, job := range []*Job{equalFence, otherThread, toolConfirmation} {
		if got := queueJobStatus(t, admin, ws, job.ID); got != StatusPending {
			t.Fatalf("%s status = %s; want pending", job.ID, got)
		}
	}
	if _, err := store.Cancel(ctx, CancelRequest{WorkspaceID: ws, SessionID: sessionID, SessionThreadID: otherThreadID, InterruptFenceSequence: 0, Now: now}); !IsValidationError(err) {
		t.Fatalf("Cancel invalid fence err = %v; want validation error", err)
	}
}

func TestPostgreSQLStoreRejectsNonCanonicalQueueShape(t *testing.T) {
	store, _ := newPostgreSQLQueueStore(t)
	ws := workspace.ID("ws_queue_shape")
	now := time.Date(2026, 7, 1, 13, 45, 0, 0, time.UTC)
	_, err := store.Enqueue(context.Background(), EnqueueRequest{
		ID:           "qjob_missing_birth",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: FormatSessionPartitionKey(ws, "sesn_shape"),
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, "sesn_shape", "input_missing_birth"),
		PayloadJSON: queuePayload(t, map[string]any{
			"workspace_id":      string(ws),
			"session_id":        "sesn_shape",
			"session_thread_id": "thrd_shape",
			"runtime_input_id":  "input_missing_birth",
			"event_ids":         []string{"ev_missing_birth"},
			"sequence_from":     1,
			"sequence_to":       1,
			"input_kind":        "messages",
		}),
		Now: now,
	})
	if !IsValidationError(err) || !strings.Contains(err.Error(), "preparation_attempt_id") {
		t.Fatalf("missing birth preparation attempt err = %v; want preparation_attempt_id validation error", err)
	}

	_, err = store.Enqueue(context.Background(), EnqueueRequest{
		ID:           "qjob_bad_shape",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: FormatSessionPartitionKey(ws, "wrong_session"),
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, "sesn_shape", "input_shape"),
		PayloadJSON:  runtimeInputPayload(t, ws, "sesn_shape", "thrd_shape", "input_shape", "messages", 1, 1),
		Now:          now,
	})
	if !IsValidationError(err) {
		t.Fatalf("non-canonical partition err = %v; want validation error", err)
	}

	_, err = store.Enqueue(context.Background(), EnqueueRequest{
		ID:           "qjob_bad_payload",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: FormatSessionPartitionKey(ws, "sesn_shape"),
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, "sesn_shape", "input_secret"),
		PayloadJSON: queuePayload(t, map[string]any{
			"workspace_id":        string(ws),
			"session_id":          "sesn_shape",
			"session_thread_id":   "thrd_shape",
			"runtime_input_id":    "input_secret",
			"event_ids":           []string{"ev_secret"},
			"sequence_from":       1,
			"sequence_to":         1,
			"input_kind":          "messages",
			"copied_user_text":    "must stay in session_events",
			"provider_credential": "must never enter queue payload",
		}),
		Now: now,
	})
	if !IsValidationError(err) {
		t.Fatalf("extra payload field err = %v; want validation error", err)
	}
}

func TestPostgreSQLStoreAcceptsRuntimeMCPManifestUpdateCanonicalShape(t *testing.T) {
	store, _ := newPostgreSQLQueueStore(t)
	ws := workspace.ID("ws_queue_mcp_manifest")
	sessionID := "sesn_manifest"
	now := time.Date(2026, 7, 1, 13, 48, 0, 0, time.UTC)
	payload := queuePayload(t, map[string]any{
		"workspace_id":        string(ws),
		"session_id":          sessionID,
		"mcp_server_name":     "github",
		"manifest_generation": 1,
	})

	job := mustEnqueue(t, store, EnqueueRequest{
		ID:             "qjob_mcp_manifest",
		WorkspaceID:    ws,
		Kind:           KindRuntimeConfigUpdate,
		PartitionKey:   FormatSessionPartitionKey(ws, sessionID),
		DedupeKey:      FormatRuntimeMCPManifestUpdateDedupeKey(ws, sessionID, "github", "1"),
		PayloadVersion: 2,
		PayloadJSON:    payload,
		Now:            now,
	})
	leased := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID:   ws,
		Kinds:         []string{KindRuntimeConfigUpdate},
		LeaseOwner:    "bridge",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(time.Second),
	})
	if leased.ID != job.ID {
		t.Fatalf("leased MCP manifest job = %s; want %s", leased.ID, job.ID)
	}
	_, err := store.Enqueue(context.Background(), EnqueueRequest{
		ID:           "qjob_mcp_bad_dedupe",
		WorkspaceID:  ws,
		Kind:         KindRuntimeConfigUpdate,
		PartitionKey: FormatSessionPartitionKey(ws, sessionID),
		DedupeKey:    FormatRuntimeConfigUpdateDedupeKey(ws, sessionID, "7"),
		PayloadJSON:  payload,
		Now:          now.Add(2 * time.Second),
	})
	if !IsValidationError(err) {
		t.Fatalf("MCP manifest bad dedupe err = %v; want validation error", err)
	}

	approvalPatch := []byte(`{"workspace_id":"ws_queue_mcp_manifest","session_id":"sesn_manifest","config_generation":8,"approval_mode":"full_access","tool_policy":{"approvalMode":"full_access","mcpToolsets":[]}}`)
	if _, err := store.Enqueue(context.Background(), EnqueueRequest{
		ID:           "qjob_config_approve",
		WorkspaceID:  ws,
		Kind:         KindRuntimeConfigUpdate,
		PartitionKey: FormatSessionPartitionKey(ws, sessionID),
		DedupeKey:    FormatRuntimeConfigUpdateDedupeKey(ws, sessionID, "8"),
		PayloadJSON:  approvalPatch,
		Now:          now.Add(3 * time.Second),
	}); err != nil {
		t.Fatalf("runtime config approval_mode enqueue: %v", err)
	}
	_, err = store.Enqueue(context.Background(), EnqueueRequest{
		ID:           "qjob_config_bad",
		WorkspaceID:  ws,
		Kind:         KindRuntimeConfigUpdate,
		PartitionKey: FormatSessionPartitionKey(ws, sessionID),
		DedupeKey:    FormatRuntimeConfigUpdateDedupeKey(ws, sessionID, "9"),
		PayloadJSON:  []byte(`{"workspace_id":"ws_queue_mcp_manifest","session_id":"sesn_manifest","config_generation":9,"approval_mode":"always"}`),
		Now:          now.Add(4 * time.Second),
	})
	if !IsValidationError(err) {
		t.Fatalf("runtime config bad approval_mode err = %v; want validation error", err)
	}
}

func TestPostgreSQLStoreAcceptsTaskNotificationRuntimeInputWithoutPublicEventFence(t *testing.T) {
	store, _ := newPostgreSQLQueueStore(t)
	ws := workspace.ID("ws_queue_task_notification")
	now := time.Date(2026, 7, 1, 13, 50, 0, 0, time.UTC)
	runtimeInputID := "task_notification:task_queue_notify"

	job := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_task_notify",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: FormatSessionPartitionKey(ws, "sesn_task_notify"),
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, "sesn_task_notify", runtimeInputID),
		PayloadJSON: queuePayload(t, map[string]any{
			"workspace_id":           string(ws),
			"session_id":             "sesn_task_notify",
			"session_thread_id":      "thrd_task_notify",
			"runtime_input_id":       runtimeInputID,
			"event_ids":              []string{},
			"input_kind":             "task_notification",
			"preparation_attempt_id": "prep_sesn_task_notify",
		}),
		Now: now,
	})
	if job.ID != "qjob_task_notify" {
		t.Fatalf("task notification job id = %s", job.ID)
	}
}

func TestPostgreSQLStoreRetryDeadLetterAndReclaimExpiredLeases(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	ws := workspace.ID("ws_queue_retry")
	retrySessionID := "sesn_retry"
	reclaimSessionID := "sesn_reclaim"
	now := time.Date(2026, 7, 1, 14, 0, 0, 0, time.UTC)

	job := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_retry",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: FormatSessionPartitionKey(ws, retrySessionID),
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, retrySessionID, "input_1"),
		PayloadJSON:  runtimeInputPayload(t, ws, retrySessionID, "thrd_retry", "input_1", "messages", 1, 1),
		MaxAttempts:  2,
		Now:          now,
	})
	firstLease := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge", MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(time.Second)})
	if ok, err := store.Retry(ctx, RetryRequest{WorkspaceID: ws, JobID: job.ID, LeaseToken: firstLease.LeaseToken, ErrorKind: "runtime_unavailable", Now: now.Add(2 * time.Second)}); err != nil || !ok {
		t.Fatalf("Retry first lease = (%v,%v); want true,nil", ok, err)
	}
	if ok, err := store.Ack(ctx, AckRequest{WorkspaceID: ws, JobID: job.ID, LeaseToken: firstLease.LeaseToken, Now: now.Add(3 * time.Second)}); err != nil || ok {
		t.Fatalf("stale Ack after retry = (%v,%v); want false,nil", ok, err)
	}
	secondLease := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge", MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(8 * time.Second)})
	if ok, err := store.Retry(ctx, RetryRequest{WorkspaceID: ws, JobID: job.ID, LeaseToken: secondLease.LeaseToken, ErrorKind: "invariant_failed", Now: now.Add(9 * time.Second)}); err != nil || !ok {
		t.Fatalf("Retry max attempt = (%v,%v); want true,nil", ok, err)
	}
	if got := queueJobStatus(t, admin, ws, job.ID); got != StatusDeadLettered {
		t.Fatalf("max-attempt retry status = %s; want dead_lettered", got)
	}

	expiring := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_reclaim",
		WorkspaceID:  ws,
		Kind:         KindCleanupSession,
		PartitionKey: FormatSessionPartitionKey(ws, reclaimSessionID),
		DedupeKey:    FormatCleanupSessionDedupeKey(ws, reclaimSessionID, "cleanup_1"),
		PayloadJSON:  []byte(`{"workspace_id":"ws_queue_retry","session_id":"sesn_reclaim","cleanup_job_id":"cleanup_1"}`),
		MaxAttempts:  2,
		Now:          now,
	})
	mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindCleanupSession}, LeaseOwner: "bridge", MaxJobs: 1, LeaseDuration: time.Second, Now: now.Add(10 * time.Second)})
	if reclaimed, err := store.ReclaimExpiredLeases(ctx, ReclaimExpiredLeasesRequest{WorkspaceID: ws, Kind: KindCleanupSession, Now: now.Add(12 * time.Second)}); err != nil || reclaimed != 1 {
		t.Fatalf("ReclaimExpiredLeases first = (%d,%v); want 1,nil", reclaimed, err)
	}
	if got := queueJobStatus(t, admin, ws, expiring.ID); got != StatusPending {
		t.Fatalf("first expired reclaim status = %s; want pending", got)
	}
	mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindCleanupSession}, LeaseOwner: "bridge", MaxJobs: 1, LeaseDuration: time.Second, Now: now.Add(13 * time.Second)})
	if reclaimed, err := store.ReclaimExpiredLeases(ctx, ReclaimExpiredLeasesRequest{WorkspaceID: ws, Kind: KindCleanupSession, Now: now.Add(15 * time.Second)}); err != nil || reclaimed != 1 {
		t.Fatalf("ReclaimExpiredLeases second = (%d,%v); want 1,nil", reclaimed, err)
	}
	if got := queueJobStatus(t, admin, ws, expiring.ID); got != StatusPending {
		t.Fatalf("second expired reclaim status = %s; want pending because lease expiration alone never dead-letters", got)
	}
}

func TestPostgreSQLStoreDeferPreservesSessionPrepareAttemptBudgetAndFencesKind(t *testing.T) {
	t.Run("session prepare returns pending without consuming an attempt", func(t *testing.T) {
		store, admin := newPostgreSQLQueueStore(t)
		store.retryPolicy.RandomInt64 = func(int64) int64 { return 0 }
		ctx := context.Background()
		ws := workspace.ID("ws_queue_defer_prepare")
		now := time.Date(2026, 7, 18, 13, 0, 0, 0, time.UTC)
		job := mustEnqueue(t, store, EnqueueRequest{
			ID:           "qjob_defer_prepare",
			WorkspaceID:  ws,
			Kind:         KindSessionPrepare,
			PartitionKey: FormatSessionPartitionKey(ws, "sesn_defer_prepare"),
			DedupeKey:    FormatSessionPrepareDedupeKey(ws, "sesn_defer_prepare", "prep_defer"),
			PayloadJSON:  []byte(`{"workspace_id":"ws_queue_defer_prepare","session_id":"sesn_defer_prepare","preparation_attempt_id":"prep_defer"}`),
			MaxAttempts:  1,
			Now:          now,
		})
		first := mustLeaseOne(t, store, LeaseRequest{
			WorkspaceID: ws, Kinds: []string{KindSessionPrepare}, LeaseOwner: "sandbox",
			MaxJobs: 1, LeaseDuration: time.Minute, Now: now,
		})
		if first.AttemptCount != 1 {
			t.Fatalf("first lease attempt_count = %d; want 1", first.AttemptCount)
		}
		if ok, err := store.Defer(ctx, DeferRequest{
			WorkspaceID: ws, JobID: job.ID, LeaseToken: first.LeaseToken, Now: now.Add(time.Second),
		}); err != nil || !ok {
			t.Fatalf("Defer = (%v,%v); want true,nil", ok, err)
		}
		var (
			status       string
			attemptCount int
			leaseToken   sql.NullString
		)
		if err := admin.QueryRowContext(ctx,
			`SELECT status, attempt_count, lease_token
			   FROM queue_jobs
			  WHERE workspace_id = $1 AND id = $2`,
			string(ws), job.ID,
		).Scan(&status, &attemptCount, &leaseToken); err != nil {
			t.Fatalf("read deferred job: %v", err)
		}
		if status != StatusPending || attemptCount != 0 || leaseToken.Valid {
			t.Fatalf("deferred row = status %q attempt %d token %#v; want pending/0/NULL", status, attemptCount, leaseToken)
		}
		if ok, err := store.Defer(ctx, DeferRequest{
			WorkspaceID: ws, JobID: job.ID, LeaseToken: first.LeaseToken, Now: now.Add(2 * time.Second),
		}); err != nil || ok {
			t.Fatalf("stale Defer = (%v,%v); want false,nil", ok, err)
		}
		second := mustLeaseOne(t, store, LeaseRequest{
			WorkspaceID: ws, Kinds: []string{KindSessionPrepare}, LeaseOwner: "sandbox",
			MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(2 * time.Second),
		})
		if second.AttemptCount != 1 || second.MaxAttempts != 1 {
			t.Fatalf("second lease attempts = %d/%d; want full 1/1 budget", second.AttemptCount, second.MaxAttempts)
		}
	})

	t.Run("other job kinds are rejected without changing the lease", func(t *testing.T) {
		store, admin := newPostgreSQLQueueStore(t)
		ctx := context.Background()
		ws := workspace.ID("ws_queue_defer_kind")
		now := time.Date(2026, 7, 18, 13, 30, 0, 0, time.UTC)
		job := mustEnqueue(t, store, EnqueueRequest{
			ID:           "qjob_defer_runtime",
			WorkspaceID:  ws,
			Kind:         KindRuntimeInput,
			PartitionKey: FormatSessionPartitionKey(ws, "sesn_defer_kind"),
			DedupeKey:    FormatRuntimeInputDedupeKey(ws, "sesn_defer_kind", "rin_defer_kind"),
			PayloadJSON:  runtimeInputPayload(t, ws, "sesn_defer_kind", "thrd_defer_kind", "rin_defer_kind", "messages", 1, 1),
			Now:          now,
		})
		leased := mustLeaseOne(t, store, LeaseRequest{
			WorkspaceID: ws, Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge",
			MaxJobs: 1, LeaseDuration: time.Minute, Now: now,
		})
		ok, err := store.Defer(ctx, DeferRequest{
			WorkspaceID: ws, JobID: job.ID, LeaseToken: leased.LeaseToken, Now: now.Add(time.Second),
		})
		var validation *ValidationError
		if ok || !errors.As(err, &validation) {
			t.Fatalf("Defer runtime_input = (%v,%T %v); want false ValidationError", ok, err, err)
		}
		if got := queueJobStatus(t, admin, ws, job.ID); got != StatusLeased {
			t.Fatalf("runtime_input status after rejected Defer = %s; want leased", got)
		}
	})
}

func TestPostgreSQLStoreLastSuccessfulHeartbeatBlocksPreparationSuccessorsPastRemoteKillBound(t *testing.T) {
	for _, successorKind := range []string{KindSessionPrepare, KindSessionDeleteCleanup} {
		t.Run(successorKind, func(t *testing.T) {
			store, _ := newPostgreSQLQueueStore(t)
			ctx := context.Background()
			ws := workspace.ID("ws_late_command_" + successorKind)
			sessionID := "sesn_late_command"
			now := time.Date(2026, 7, 14, 18, 0, 0, 0, time.UTC)
			mustEnqueue(t, store, EnqueueRequest{ID: "qjob_old_prepare", WorkspaceID: ws, Kind: KindSessionPrepare, PartitionKey: FormatSessionPartitionKey(ws, sessionID), DedupeKey: FormatSessionPrepareDedupeKey(ws, sessionID, "prep_old"), PayloadJSON: []byte(`{"workspace_id":"` + string(ws) + `","session_id":"sesn_late_command","preparation_attempt_id":"prep_old"}`), Now: now})
			successorDedupe := FormatSessionPrepareDedupeKey(ws, sessionID, "prep_successor")
			successorPayload := []byte(`{"workspace_id":"` + string(ws) + `","session_id":"sesn_late_command","preparation_attempt_id":"prep_successor"}`)
			if successorKind == KindSessionDeleteCleanup {
				successorDedupe = FormatSessionDeleteCleanupDedupeKey(ws, sessionID, "delcln_successor")
				successorPayload = []byte(`{"workspace_id":"` + string(ws) + `","session_id":"sesn_late_command","delete_cleanup_id":"delcln_successor"}`)
			}
			mustEnqueue(t, store, EnqueueRequest{ID: "qjob_successor", WorkspaceID: ws, Kind: successorKind, PartitionKey: FormatSessionPartitionKey(ws, sessionID), DedupeKey: successorDedupe, PayloadJSON: successorPayload, Now: now.Add(time.Second)})
			old := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindSessionPrepare}, LeaseOwner: "sandbox-old", MaxJobs: 1, LeaseDuration: 120 * time.Second, Now: now.Add(2 * time.Second)})
			lastHeartbeat := now.Add(15 * time.Second)
			if ok, err := store.Heartbeat(ctx, HeartbeatRequest{WorkspaceID: ws, JobID: old.ID, LeaseToken: old.LeaseToken, LeaseDuration: 120 * time.Second, Now: lastHeartbeat}); err != nil || !ok {
				t.Fatalf("last successful heartbeat = (%v,%v)", ok, err)
			}
			for _, attemptAt := range []time.Time{lastHeartbeat.Add(74 * time.Second), lastHeartbeat.Add(119 * time.Second)} {
				leased, err := store.Lease(ctx, LeaseRequest{WorkspaceID: ws, Kinds: []string{successorKind}, LeaseOwner: "successor", MaxJobs: 1, LeaseDuration: 120 * time.Second, Now: attemptAt})
				if err != nil || len(leased) != 0 {
					t.Fatalf("successor at %s = %+v err=%v; want blocked after last heartbeat through timeout+margin and lease", attemptAt.Sub(lastHeartbeat), leased, err)
				}
			}
			if ok, err := store.Ack(ctx, AckRequest{WorkspaceID: ws, JobID: old.ID, LeaseToken: old.LeaseToken, Now: lastHeartbeat.Add(120 * time.Second)}); err != nil || !ok {
				t.Fatalf("old terminal ACK = (%v,%v)", ok, err)
			}
			got := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{successorKind}, LeaseOwner: "successor", MaxJobs: 1, LeaseDuration: 120 * time.Second, Now: lastHeartbeat.Add(120 * time.Second)})
			if got.ID != "qjob_successor" {
				t.Fatalf("successor lease = %s; want qjob_successor", got.ID)
			}
		})
	}
}

func TestQueueRetryDelayUsesFullJitterAndExponentialCap(t *testing.T) {
	var bounds []int64
	policy := normalizeRetryPolicy(RetryPolicy{
		BaseDelay:   time.Second,
		MaxDelay:    5 * time.Second,
		MaxAttempts: 7,
		RandomInt64: func(bound int64) int64 {
			bounds = append(bounds, bound)
			return bound - 1
		},
	})

	for attempt := 1; attempt <= 5; attempt++ {
		_ = queueRetryDelay(policy, attempt)
	}

	want := []int64{
		int64(time.Second) + 1,
		int64(2*time.Second) + 1,
		int64(4*time.Second) + 1,
		int64(5*time.Second) + 1,
		int64(5*time.Second) + 1,
	}
	if !reflect.DeepEqual(bounds, want) {
		t.Fatalf("jitter bounds = %v; want %v", bounds, want)
	}
}

func TestPostgreSQLStoreProjectsUnsetMaxAttemptsAndUsesItForRetry(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	store.retryPolicy.MaxAttempts = 2
	store.retryPolicy.RandomInt64 = func(int64) int64 { return 0 }
	ctx := context.Background()
	ws := workspace.ID("ws_queue_default_attempts")
	now := time.Date(2026, 7, 1, 15, 0, 0, 0, time.UTC)
	job := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_default_attempts",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: FormatSessionPartitionKey(ws, "sesn_default_attempts"),
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, "sesn_default_attempts", "rin_default_attempts"),
		PayloadJSON:  runtimeInputPayload(t, ws, "sesn_default_attempts", "thrd_default_attempts", "rin_default_attempts", "messages", 1, 1),
		MaxAttempts:  0,
		Now:          now,
	})
	var storedMaxAttempts int
	if err := admin.QueryRow(`SELECT max_attempts FROM queue_jobs WHERE workspace_id = $1 AND id = $2`, string(ws), job.ID).Scan(&storedMaxAttempts); err != nil {
		t.Fatalf("read durable max_attempts: %v", err)
	}
	if storedMaxAttempts != 0 {
		t.Fatalf("durable max_attempts = %d; want unset 0", storedMaxAttempts)
	}
	first := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge", MaxJobs: 1, LeaseDuration: time.Minute, Now: now})
	if first.MaxAttempts != 2 {
		t.Fatalf("leased max_attempts = %d; want effective default 2", first.MaxAttempts)
	}
	if ok, err := store.Retry(ctx, RetryRequest{WorkspaceID: ws, JobID: job.ID, LeaseToken: first.LeaseToken, Now: now}); err != nil || !ok {
		t.Fatalf("first Retry = (%v,%v); want true,nil", ok, err)
	}
	second := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge", MaxJobs: 1, LeaseDuration: time.Minute, Now: now})
	if ok, err := store.Retry(ctx, RetryRequest{WorkspaceID: ws, JobID: job.ID, LeaseToken: second.LeaseToken, Now: now}); err != nil || !ok {
		t.Fatalf("second Retry = (%v,%v); want true,nil", ok, err)
	}
	if got := queueJobStatus(t, admin, ws, job.ID); got != StatusDeadLettered {
		t.Fatalf("effective-default retry status = %s; want dead_lettered", got)
	}
}

func TestPostgreSQLStoreMetricsSummarizesQueueState(t *testing.T) {
	store, _ := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	ws := workspace.ID("ws_queue_metrics")
	now := time.Date(2026, 7, 1, 16, 0, 0, 0, time.UTC)

	mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_metric_old",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: FormatSessionPartitionKey(ws, "sesn_metrics_old"),
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, "sesn_metrics_old", "input_old"),
		PayloadJSON:  runtimeInputPayload(t, ws, "sesn_metrics_old", "thrd_metrics_old", "input_old", "messages", 1, 1),
		AvailableAt:  now.Add(-30 * time.Second),
		Now:          now.Add(-30 * time.Second),
	})
	retryJob := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_metrics_retry",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: FormatSessionPartitionKey(ws, "sesn_metrics_retry"),
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, "sesn_metrics_retry", "input_retry"),
		PayloadJSON:  runtimeInputPayload(t, ws, "sesn_metrics_retry", "thrd_metrics_retry", "input_retry", "messages", 1, 1),
		MaxAttempts:  3,
		Priority:     100,
		Now:          now.Add(-20 * time.Second),
	})
	retryLease := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge", MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(-19 * time.Second)})
	if retryLease.ID != retryJob.ID {
		t.Fatalf("retry lease id = %s; want %s", retryLease.ID, retryJob.ID)
	}
	if ok, err := store.Retry(ctx, RetryRequest{WorkspaceID: ws, JobID: retryJob.ID, LeaseToken: retryLease.LeaseToken, ErrorKind: "runtime_unavailable", Now: now.Add(-18 * time.Second)}); err != nil || !ok {
		t.Fatalf("Retry metrics job = (%v,%v); want true,nil", ok, err)
	}
	leasedCleanup := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_metric_cleanup",
		WorkspaceID:  ws,
		Kind:         KindCleanupSession,
		PartitionKey: FormatSessionPartitionKey(ws, "sesn_metrics_cleanup"),
		DedupeKey:    FormatCleanupSessionDedupeKey(ws, "sesn_metrics_cleanup", "cleanup_1"),
		PayloadJSON:  []byte(`{"workspace_id":"ws_queue_metrics","session_id":"sesn_metrics_cleanup","cleanup_job_id":"cleanup_1"}`),
		Now:          now.Add(-10 * time.Second),
	})
	cleanupLease := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindCleanupSession}, LeaseOwner: "bridge", MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(-9 * time.Second)})
	if cleanupLease.ID != leasedCleanup.ID {
		t.Fatalf("cleanup lease id = %s; want %s", cleanupLease.ID, leasedCleanup.ID)
	}
	deadJob := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_metrics_dead",
		WorkspaceID:  ws,
		Kind:         KindSessionPrepare,
		PartitionKey: FormatSessionPartitionKey(ws, "sesn_metrics_dead"),
		DedupeKey:    FormatSessionPrepareDedupeKey(ws, "sesn_metrics_dead", "prep_dead"),
		PayloadJSON:  []byte(`{"workspace_id":"ws_queue_metrics","session_id":"sesn_metrics_dead","preparation_attempt_id":"prep_dead"}`),
		MaxAttempts:  1,
		Now:          now.Add(-8 * time.Second),
	})
	deadLease := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindSessionPrepare}, LeaseOwner: "sandbox", MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(-7 * time.Second)})
	if deadLease.ID != deadJob.ID {
		t.Fatalf("dead lease id = %s; want %s", deadLease.ID, deadJob.ID)
	}
	if ok, err := store.Retry(ctx, RetryRequest{WorkspaceID: ws, JobID: deadJob.ID, LeaseToken: deadLease.LeaseToken, ErrorKind: "prepare_failed", Now: now.Add(-6 * time.Second)}); err != nil || !ok {
		t.Fatalf("Retry dead metrics job = (%v,%v); want true,nil", ok, err)
	}

	snapshots, err := store.Metrics(ctx, now)
	if err != nil {
		t.Fatalf("Metrics: %v", err)
	}
	byKind := map[string]MetricsSnapshot{}
	for _, snapshot := range snapshots {
		byKind[snapshot.Kind] = snapshot
	}
	runtimeInput := byKind[KindRuntimeInput]
	if runtimeInput.PendingJobs != 2 || runtimeInput.RetryPendingJobs != 1 || runtimeInput.LeasedJobs != 0 || runtimeInput.DeadLetteredJobs != 0 {
		t.Fatalf("runtime_input metrics = %+v", runtimeInput)
	}
	if runtimeInput.ReadyLagSeconds < 29.9 || runtimeInput.ReadyLagSeconds > 30.1 {
		t.Fatalf("runtime_input ready lag = %f; want about 30s", runtimeInput.ReadyLagSeconds)
	}
	cleanup := byKind[KindCleanupSession]
	if cleanup.LeasedJobs != 1 || cleanup.PendingJobs != 0 {
		t.Fatalf("cleanup metrics = %+v", cleanup)
	}
	prepare := byKind[KindSessionPrepare]
	if prepare.DeadLetteredJobs != 1 || prepare.PendingJobs != 0 {
		t.Fatalf("session_prepare metrics = %+v", prepare)
	}
}

func TestPostgreSQLStoreReclaimsExpiredLeasesAcrossWorkspacesWithQueueMaintenancePolicy(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 1, 15, 0, 0, 0, time.UTC)
	for _, ws := range []workspace.ID{"ws_queue_maint_a", "ws_queue_maint_b"} {
		mustEnqueue(t, store, EnqueueRequest{
			ID:           "qjob_" + string(ws),
			WorkspaceID:  ws,
			Kind:         KindRuntimeInput,
			PartitionKey: FormatSessionPartitionKey(ws, "sesn_maint"),
			DedupeKey:    FormatRuntimeInputDedupeKey(ws, "sesn_maint", "input_1"),
			PayloadJSON:  runtimeInputPayload(t, ws, "sesn_maint", "thrd_maint", "input_1", "messages", 1, 1),
			Now:          now,
		})
		mustLeaseOne(t, store, LeaseRequest{
			WorkspaceID:   ws,
			Kinds:         []string{KindRuntimeInput},
			LeaseOwner:    "bridge",
			MaxJobs:       1,
			LeaseDuration: time.Second,
			Now:           now.Add(time.Second),
		})
	}
	reclaimed, err := store.ReclaimExpiredLeases(ctx, ReclaimExpiredLeasesRequest{
		Limit: 10,
		Now:   now.Add(3 * time.Second),
	})
	if err != nil || reclaimed != 2 {
		t.Fatalf("global ReclaimExpiredLeases = (%d,%v); want 2,nil", reclaimed, err)
	}
	for _, ws := range []workspace.ID{"ws_queue_maint_a", "ws_queue_maint_b"} {
		if got := queueJobStatus(t, admin, ws, "qjob_"+string(ws)); got != StatusPending {
			t.Fatalf("%s reclaimed status = %s; want pending", ws, got)
		}
	}
}

func newPostgreSQLQueueStore(t testing.TB) (*PostgreSQLQueueStore, *sql.DB) {
	t.Helper()
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	return NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime)), admin
}

func mustEnqueue(t testing.TB, store *PostgreSQLQueueStore, request EnqueueRequest) *Job {
	t.Helper()
	job, err := store.Enqueue(context.Background(), request)
	if err != nil {
		t.Fatalf("Enqueue(%s): %v", request.ID, err)
	}
	return job
}

func runtimeInputPayload(t testing.TB, ws workspace.ID, sessionID string, threadID string, runtimeInputID string, inputKind string, sequenceFrom int64, sequenceTo int64) []byte {
	t.Helper()
	return queuePayload(t, map[string]any{
		"workspace_id":           string(ws),
		"session_id":             sessionID,
		"session_thread_id":      threadID,
		"runtime_input_id":       runtimeInputID,
		"event_ids":              []string{"ev_" + runtimeInputID},
		"sequence_from":          sequenceFrom,
		"sequence_to":            sequenceTo,
		"input_kind":             inputKind,
		"preparation_attempt_id": "prep_" + sessionID,
	})
}

func queuePayload(t testing.TB, payload map[string]any) []byte {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	return body
}

func mustLeaseOne(t testing.TB, store *PostgreSQLQueueStore, request LeaseRequest) *Job {
	t.Helper()
	leased, err := store.Lease(context.Background(), request)
	if err != nil {
		t.Fatalf("Lease(%v): %v", request.Kinds, err)
	}
	if len(leased) != 1 {
		t.Fatalf("Lease(%v) returned %d jobs; want 1", request.Kinds, len(leased))
	}
	return leased[0]
}

func assertLeasedIDs(t testing.TB, jobs []*Job, want []string) {
	t.Helper()
	if len(jobs) != len(want) {
		t.Fatalf("leased job count = %d; want %d (%v)", len(jobs), len(want), want)
	}
	gotIDs := make([]string, 0, len(jobs))
	for index, job := range jobs {
		gotIDs = append(gotIDs, job.ID)
		if job.Status != StatusLeased || job.LeaseToken == "" || job.LeasedBy == "" || job.LeasedUntil == nil {
			t.Fatalf("leased[%d] has invalid lease shape: %#v", index, job)
		}
	}
	if !reflect.DeepEqual(gotIDs, want) {
		t.Fatalf("leased IDs = %v; want %v", gotIDs, want)
	}
}

func queueJobStatus(t testing.TB, db *sql.DB, ws workspace.ID, jobID string) string {
	t.Helper()
	var status string
	if err := db.QueryRowContext(context.Background(),
		`SELECT status FROM queue_jobs WHERE workspace_id = $1 AND id = $2`,
		string(ws),
		jobID,
	).Scan(&status); err != nil {
		t.Fatalf("read queue job status %s: %v", jobID, err)
	}
	return status
}

func countQueueJobsByDedupe(t testing.TB, db *sql.DB, ws workspace.ID, dedupe string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM queue_jobs WHERE workspace_id = $1 AND dedupe_key = $2`,
		string(ws),
		dedupe,
	).Scan(&count); err != nil {
		t.Fatalf("count queue jobs for dedupe %s: %v", dedupe, err)
	}
	return count
}
