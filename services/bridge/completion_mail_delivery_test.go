package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
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
)

func TestPostgreSQLCompletionMailReconcilerUsesInclusiveAgeAndPerRecipientPageBound(t *testing.T) {
	t.Run("inclusive age boundary", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const (
			sessionID = "sesn_completion_floor_age"
			mainID    = "thrd_completion_floor_age_main"
			childID   = "thrd_completion_floor_age_child"
		)
		seedBridgeAPISession(t, admin, "default", sessionID, mainID)
		seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
		seedCompletionMailSentAt(t, admin, sessionID, mainID, childID, "delivery_completion_floor_at_cutoff", 1, "2026-01-01T00:05:00Z")
		seedCompletionMailSentAt(t, admin, sessionID, mainID, childID, "delivery_completion_floor_younger", 2, "2026-01-01T00:05:01Z")

		store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
		store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC) }
		repaired, err := store.RepairCompletionMail(context.Background(), "default", defaultRuntimeInboxRepairBatch)
		if err != nil {
			t.Fatalf("RepairCompletionMail age boundary: %v", err)
		}
		if repaired != 1 {
			t.Fatalf("age-boundary repaired = %d; want 1", repaired)
		}
		assertActiveCompletionWake(t, admin, sessionID, "delivery_completion_floor_at_cutoff", true)
		assertActiveCompletionWake(t, admin, sessionID, "delivery_completion_floor_younger", false)
	})

	t.Run("one page per recipient", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const (
			sessionID = "sesn_completion_floor_page"
			mainID    = "thrd_completion_floor_page_main"
			childID   = "thrd_completion_floor_page_child"
		)
		seedBridgeAPISession(t, admin, "default", sessionID, mainID)
		seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
		for index := 1; index <= MailFetchMaxEnvelopes+1; index++ {
			seedCompletionMailSentAt(
				t,
				admin,
				sessionID,
				mainID,
				childID,
				"delivery_completion_floor_page_"+strconv.Itoa(index),
				int64(index),
				"2026-01-01T00:00:00Z",
			)
		}

		store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
		store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC) }
		repaired, err := store.RepairCompletionMail(context.Background(), "default", defaultRuntimeInboxRepairBatch)
		if err != nil {
			t.Fatalf("RepairCompletionMail page bound: %v", err)
		}
		if repaired != MailFetchMaxEnvelopes {
			t.Fatalf("page-bound repaired = %d; want %d", repaired, MailFetchMaxEnvelopes)
		}
		for index := 1; index <= MailFetchMaxEnvelopes+1; index++ {
			assertActiveCompletionWake(
				t,
				admin,
				sessionID,
				"delivery_completion_floor_page_"+strconv.Itoa(index),
				index <= MailFetchMaxEnvelopes,
			)
		}
	})

	t.Run("global pass bound", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const (
			sessionID = "sesn_completion_floor_global"
			mainID    = "thrd_completion_floor_global_main"
		)
		seedBridgeAPISession(t, admin, "default", sessionID, mainID)
		for index := 1; index <= defaultRuntimeInboxRepairBatch+1; index++ {
			parentID := "thrd_completion_floor_parent_" + strconv.Itoa(index)
			sourceID := "thrd_completion_floor_source_" + strconv.Itoa(index)
			seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, parentID)
			seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, sourceID)
			seedCompletionMailSentAt(
				t,
				admin,
				sessionID,
				parentID,
				sourceID,
				"delivery_completion_floor_global_"+strconv.Itoa(index),
				1,
				"2026-01-01T00:00:00Z",
			)
		}

		store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
		store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC) }
		repaired, err := store.RepairCompletionMail(context.Background(), "default", defaultRuntimeInboxRepairBatch)
		if err != nil {
			t.Fatalf("RepairCompletionMail global bound: %v", err)
		}
		if repaired != defaultRuntimeInboxRepairBatch {
			t.Fatalf("global-bound repaired = %d; want %d", repaired, defaultRuntimeInboxRepairBatch)
		}
		var active int
		if err := admin.QueryRowContext(context.Background(),
			`SELECT count(*)
			   FROM queue_jobs
			  WHERE workspace_id = 'default'
			    AND status IN ('pending', 'leased')
			    AND payload_json::jsonb ->> 'input_kind' = 'agent_mail'
			    AND payload_json::jsonb ->> 'session_id' = $1`,
			sessionID,
		).Scan(&active); err != nil {
			t.Fatalf("count global-bound completion wakes: %v", err)
		}
		if active != defaultRuntimeInboxRepairBatch {
			t.Fatalf("global-bound active wakes = %d; want %d", active, defaultRuntimeInboxRepairBatch)
		}
	})

	t.Run("recovers parent-to-child instruction", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const (
			sessionID = "sesn_agent_mail_direct_reconcile"
			mainID    = "thrd_agent_mail_direct_reconcile_main"
			childID   = "thrd_agent_mail_direct_reconcile_child"
			delivery  = "delivery_agent_mail_direct_reconcile"
		)
		seedBridgeAPISession(t, admin, "default", sessionID, mainID)
		seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
		seedCompletionMailSentAt(t, admin, sessionID, childID, mainID, delivery, 1, "2026-01-01T00:00:00Z")

		store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
		store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC) }
		repaired, err := store.RepairCompletionMail(context.Background(), "default", defaultRuntimeInboxRepairBatch)
		if err != nil || repaired != 1 {
			t.Fatalf("repair stranded direct instruction = %d/%v; want 1/nil", repaired, err)
		}
		assertActiveCompletionWake(t, admin, sessionID, delivery, true)
	})
}

func TestPostgreSQLCompletionMailReconcilerWaitsForActiveWakeThenRearmsAfterUnreceiptedAck(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_completion_floor_race"
		mainID    = "thrd_completion_floor_race_main"
		childID   = "thrd_completion_floor_race_child"
		delivery  = "delivery_completion_floor_race"
	)
	now := time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedCompletionMailSentAt(t, admin, sessionID, mainID, childID, delivery, 1, "2026-01-01T00:00:00Z")

	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	runtimeInputID := completionRuntimeInputID(delivery)
	job, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
		WorkspaceID:    workspace.ID("default"),
		Kind:           queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.ID("default"), sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.ID("default"), sessionID, runtimeInputID),
		PayloadVersion: 1,
		PayloadJSON:    completionMailQueuePayload(t, sessionID, mainID, runtimeInputID),
		MaxAttempts:    queue.DefaultMaxAttempts,
		Now:            now.Add(-time.Minute),
	})
	if err != nil {
		t.Fatalf("enqueue initial completion wake: %v", err)
	}
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID:   workspace.ID("default"),
		Kinds:         []string{queue.KindRuntimeInput},
		LeaseOwner:    "completion-floor-race",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now,
	})
	if err != nil || len(leased) != 1 || leased[0].ID != job.ID {
		t.Fatalf("lease initial completion wake = %#v/%v; want one leased wake", leased, err)
	}

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return now }
	if repaired, err := store.RepairCompletionMail(context.Background(), "default", defaultRuntimeInboxRepairBatch); err != nil || repaired != 0 {
		t.Fatalf("repair while birth leased = %d/%v; want 0/nil", repaired, err)
	}
	if acknowledged, err := queueStore.Ack(context.Background(), queue.AckRequest{
		WorkspaceID: workspace.ID("default"),
		JobID:       leased[0].ID,
		LeaseToken:  leased[0].LeaseToken,
		Now:         now.Add(time.Second),
	}); err != nil || !acknowledged {
		t.Fatalf("ack unreceipted initial wake = %v/%v; want true/nil", acknowledged, err)
	}
	if repaired, err := store.RepairCompletionMail(context.Background(), "default", defaultRuntimeInboxRepairBatch); err != nil || repaired != 1 {
		t.Fatalf("repair after unreceipted ACK = %d/%v; want 1/nil", repaired, err)
	}
	assertActiveCompletionWake(t, admin, sessionID, delivery, true)
}

func TestPostgreSQLAgentMailInboxRepairRedeliversAfterAcceptedQueueAck(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID  = "sesn_agent_mail_inbox_repair"
		mainID     = "thrd_agent_mail_inbox_repair_main"
		childID    = "thrd_agent_mail_inbox_repair_child"
		deliveryID = "delivery_agent_mail_inbox_repair"
		bindingID  = "bind_agent_mail_inbox_repair"
		podUID     = "pod_agent_mail_inbox_repair"
		prepID     = "prep_agent_mail_inbox_repair"
	)
	now := time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedCompletionMailSentAt(t, admin, sessionID, mainID, childID, deliveryID, 1, "2026-01-01T00:00:00Z")

	apiStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	apiStore.Clock = func() time.Time { return now }
	apiStore.RuntimeBindingTokenHMACKey = []byte("agent-mail-inbox-repair-key-32b")
	scope := bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID)
	resolved, err := apiStore.ResolveInterAgentDelivery(context.Background(), &bridgev1.ResolveInterAgentDeliveryRequest{
		Scope:         scope,
		ChildThreadId: childID,
	})
	if err != nil {
		t.Fatalf("ResolveInterAgentDelivery: %v", err)
	}

	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID:   workspace.ID("default"),
		Kinds:         []string{queue.KindRuntimeInput},
		LeaseOwner:    "agent-mail-inbox-repair-first",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now,
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease initial agent-mail wake = %#v/%v; want one", leased, err)
	}
	initialJob, err := DecodeRuntimeJob(queueJobProto(leased[0]))
	if err != nil {
		t.Fatalf("decode initial agent-mail wake: %v", err)
	}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	deliveryStore.Clock = func() time.Time { return now }
	initialPlan, err := deliveryStore.PrepareRuntimeCommand(context.Background(), initialJob)
	if err != nil || initialPlan.Request == nil {
		t.Fatalf("prepare initial agent-mail wake = %#v/%v; want Runtime command", initialPlan, err)
	}
	if err := deliveryStore.MarkRuntimeInputAccepted(context.Background(), initialJob, initialPlan.Request); err != nil {
		t.Fatalf("mark initial agent-mail accepted: %v", err)
	}
	if acknowledged, err := queueStore.Ack(context.Background(), queue.AckRequest{
		WorkspaceID: workspace.ID("default"),
		JobID:       leased[0].ID,
		LeaseToken:  leased[0].LeaseToken,
		Now:         now.Add(time.Second),
	}); err != nil || !acknowledged {
		t.Fatalf("ack accepted agent-mail wake = %v/%v; want true/nil", acknowledged, err)
	}

	recoveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	recoveryStore.Clock = func() time.Time { return now.Add(2 * time.Second) }
	if repaired, err := recoveryStore.RepairRuntimeInbox(context.Background(), "default", defaultRuntimeInboxRepairBatch); err != nil || repaired != 1 {
		t.Fatalf("repair accepted agent-mail inbox = %d/%v; want 1/nil", repaired, err)
	}
	if repaired, err := recoveryStore.RepairRuntimeInbox(context.Background(), "default", defaultRuntimeInboxRepairBatch); err != nil || repaired != 0 {
		t.Fatalf("repair accepted agent-mail inbox with active wake = %d/%v; want 0/nil", repaired, err)
	}
	redelivery, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID:   workspace.ID("default"),
		Kinds:         []string{queue.KindRuntimeInput},
		LeaseOwner:    "agent-mail-inbox-repair-second",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(3 * time.Second),
	})
	if err != nil || len(redelivery) != 1 {
		t.Fatalf("lease repaired agent-mail wake = %#v/%v; want one", redelivery, err)
	}
	repairedJob, err := DecodeRuntimeJob(queueJobProto(redelivery[0]))
	if err != nil {
		t.Fatalf("decode repaired agent-mail wake: %v", err)
	}
	repairedPlan, err := recoveryStore.PrepareRuntimeCommand(context.Background(), repairedJob)
	if err != nil || repairedPlan.Request == nil {
		t.Fatalf("prepare repaired agent-mail wake = %#v/%v; want Runtime command", repairedPlan, err)
	}
	if repairedPlan.Request.GetPayloadJson() != initialPlan.Request.GetPayloadJson() ||
		!reflect.DeepEqual(repairedPlan.Request.GetEventIds(), initialPlan.Request.GetEventIds()) ||
		repairedPlan.Request.GetSequenceFrom() != initialPlan.Request.GetSequenceFrom() {
		t.Fatalf("repaired agent-mail command = %#v; want stable original %#v", repairedPlan.Request, initialPlan.Request)
	}
	if err := recoveryStore.MarkRuntimeInputAccepted(context.Background(), repairedJob, repairedPlan.Request); err != nil {
		t.Fatalf("mark repaired agent-mail accepted: %v", err)
	}
	draft := bridgeInputDraftForTest(
		"default",
		sessionID,
		mainID,
		"agent_mail",
		completionRuntimeInputID(deliveryID),
		resolved.GetReceivedEventId(),
		bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_AGENT_MAIL_INPUT,
		"user",
		completionMailEnvelope("main", "sender", deliveryID),
	)
	draft.MessageInfoJson = `{"role":"user","origin":"runtime","status":"completed"}`
	if _, err := apiStore.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope:          scope,
		RuntimeInputId: completionRuntimeInputID(deliveryID),
		InputKind:      "agent_mail",
		EventIds:       []string{resolved.GetReceivedEventId()},
		SequenceFrom:   resolved.GetReceivedSequence(),
		SequenceTo:     resolved.GetReceivedSequence(),
		Drafts:         []*bridgev1.RuntimeMessageDraft{draft},
	}); err != nil {
		t.Fatalf("commit repaired agent-mail input: %v", err)
	}
	var projectionCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_messages
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND session_thread_id = $2
		    AND kind = 'user'
		    AND source_event_id = $3`,
		sessionID,
		mainID,
		resolved.GetReceivedEventId(),
	).Scan(&projectionCount); err != nil {
		t.Fatalf("count repaired agent-mail projection: %v", err)
	}
	if projectionCount != 1 {
		t.Fatalf("repaired agent-mail projections = %d; want exactly one", projectionCount)
	}
}

func TestPostgreSQLCompletionMailReconcilerNeverRearmsExhaustedDelivery(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_completion_floor_exhausted"
		mainID    = "thrd_completion_floor_exhausted_main"
		childID   = "thrd_completion_floor_exhausted_child"
		delivery  = "delivery_completion_floor_exhausted"
	)
	now := time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_completion_floor_exhausted", 1, "pod_completion_floor_exhausted")
	seedCompletionMailSentAt(t, admin, sessionID, mainID, childID, delivery, 1, "2026-01-01T00:00:00Z")

	runtimeInputID := completionRuntimeInputID(delivery)
	apiStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	apiStore.Clock = func() time.Time { return now }
	if _, err := apiStore.ResolveInterAgentDelivery(context.Background(), &bridgev1.ResolveInterAgentDeliveryRequest{
		Scope:         bridgeAPIScope(sessionID, mainID, "bind_completion_floor_exhausted", 1, "pod_completion_floor_exhausted"),
		ChildThreadId: childID,
		DeliveryId:    delivery,
	}); err != nil {
		t.Fatalf("admit completion before exhaustion: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID:   workspace.ID("default"),
		Kinds:         []string{queue.KindRuntimeInput},
		LeaseOwner:    "agent-mail-exhaustion",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now,
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease exhausted completion wake = %#v/%v; want one", leased, err)
	}
	job, err := DecodeRuntimeJob(queueJobProto(leased[0]))
	if err != nil {
		t.Fatalf("decode exhausted completion wake: %v", err)
	}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return now }
	finalized, err := store.FinalizeRuntimeDelivery(context.Background(), job, RuntimeDeliveryResult{
		Status:    RuntimeDeliveryRejected,
		Retryable: true,
		ErrorKind: "runtime_busy",
	})
	if err != nil {
		t.Fatalf("finalize exhausted completion delivery: %v", err)
	}
	if finalized.ErrorKind != "runtime_delivery_exhausted" {
		t.Fatalf("finalized completion delivery = %#v; want runtime_delivery_exhausted", finalized)
	}
	if transitioned, err := queueStore.DeadLetter(context.Background(), queue.DeadLetterRequest{
		WorkspaceID:  workspace.ID("default"),
		JobID:        leased[0].ID,
		LeaseToken:   leased[0].LeaseToken,
		ErrorKind:    finalized.ErrorKind,
		ErrorMessage: finalized.ErrorMessage,
		Now:          now.Add(time.Second),
	}); err != nil || !transitioned {
		t.Fatalf("dead-letter exhausted completion wake = %v/%v; want true/nil", transitioned, err)
	}
	var deadLettered int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM queue_jobs
		  WHERE workspace_id = 'default'
		    AND dedupe_key = $1
		    AND status = 'dead_lettered'`,
		queue.FormatRuntimeInputDedupeKey(workspace.ID("default"), sessionID, runtimeInputID),
	).Scan(&deadLettered); err != nil {
		t.Fatalf("count dead-lettered completion jobs: %v", err)
	}
	if deadLettered != 1 {
		t.Fatalf("dead-lettered completion jobs = %d; want 1", deadLettered)
	}
	var inboxStatus string
	var receivedProcessed bool
	if err := admin.QueryRowContext(context.Background(),
		`SELECT inbox.status,
		        received.processed_at IS NOT NULL
		   FROM session_runtime_inbox inbox
		   JOIN session_events received
		     ON received.workspace_id = inbox.workspace_id
		    AND received.session_id = inbox.session_id
		    AND received.session_thread_id = inbox.session_thread_id
		    AND received.event_id = (inbox.event_ids_json::jsonb ->> 0)
		  WHERE inbox.workspace_id = 'default'
		    AND inbox.session_id = $1
		    AND inbox.runtime_input_id = $2`,
		sessionID,
		runtimeInputID,
	).Scan(&inboxStatus, &receivedProcessed); err != nil {
		t.Fatalf("read exhausted completion custody: %v", err)
	}
	if inboxStatus != "dead_lettered" || !receivedProcessed {
		t.Fatalf("exhausted completion custody = %q/%v; want dead_lettered/processed", inboxStatus, receivedProcessed)
	}
	if repaired, err := store.RepairRuntimeInbox(context.Background(), "default", defaultRuntimeInboxRepairBatch); err != nil || repaired != 0 {
		t.Fatalf("repair exhausted runtime inbox = %d/%v; want 0/nil", repaired, err)
	}
	if repaired, err := store.RepairCompletionMail(context.Background(), "default", defaultRuntimeInboxRepairBatch); err != nil || repaired != 0 {
		t.Fatalf("repair exhausted completion = %d/%v; want 0/nil", repaired, err)
	}
	assertActiveCompletionWake(t, admin, sessionID, delivery, false)
}

func TestPostgreSQLAgentMailExhaustionBeforeAdmissionCannotBeRearmed(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_agent_mail_pre_admission_exhaustion"
		mainID    = "thrd_agent_mail_pre_admission_exhaustion_main"
		childID   = "thrd_agent_mail_pre_admission_exhaustion_child"
		delivery  = "delivery_agent_mail_pre_admission_exhaustion"
		bindingID = "bind_agent_mail_pre_admission_exhaustion"
		podUID    = "pod_agent_mail_pre_admission_exhaustion"
	)
	now := time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedCompletionMailSentAt(t, admin, sessionID, mainID, childID, delivery, 1, "2026-01-01T00:00:00Z")

	runtimeInputID := completionRuntimeInputID(delivery)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	if _, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
		WorkspaceID:    workspace.ID("default"),
		Kind:           queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.ID("default"), sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.ID("default"), sessionID, runtimeInputID),
		PayloadVersion: 1,
		PayloadJSON:    completionMailQueuePayload(t, sessionID, mainID, runtimeInputID),
		MaxAttempts:    1,
		Now:            now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("enqueue pre-admission completion wake: %v", err)
	}
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID:   workspace.ID("default"),
		Kinds:         []string{queue.KindRuntimeInput},
		LeaseOwner:    "agent-mail-pre-admission-exhaustion",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now,
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease pre-admission completion wake = %#v/%v; want one", leased, err)
	}
	job, err := DecodeRuntimeJob(queueJobProto(leased[0]))
	if err != nil {
		t.Fatalf("decode pre-admission completion wake: %v", err)
	}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	deliveryStore.Clock = func() time.Time { return now }
	finalized, err := deliveryStore.FinalizeRuntimeDelivery(context.Background(), job, retryableExhaustionResult())
	if err != nil || finalized.ErrorKind != "runtime_delivery_exhausted" {
		t.Fatalf("finalize pre-admission completion wake = %#v/%v; want exhausted", finalized, err)
	}
	if transitioned, err := queueStore.DeadLetter(context.Background(), queue.DeadLetterRequest{
		WorkspaceID:  workspace.ID("default"),
		JobID:        leased[0].ID,
		LeaseToken:   leased[0].LeaseToken,
		ErrorKind:    finalized.ErrorKind,
		ErrorMessage: finalized.ErrorMessage,
		Now:          now.Add(time.Second),
	}); err != nil || !transitioned {
		t.Fatalf("dead-letter pre-admission completion wake = %v/%v; want true/nil", transitioned, err)
	}

	apiStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	apiStore.Clock = func() time.Time { return now.Add(2 * time.Second) }
	_, err = apiStore.ResolveInterAgentDelivery(context.Background(), &bridgev1.ResolveInterAgentDeliveryRequest{
		Scope:         bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID),
		ChildThreadId: childID,
		DeliveryId:    delivery,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("resolve exhausted pre-admission completion = %v; want FAILED_PRECONDITION", err)
	}
	var receivedCount, inboxCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT
			(SELECT count(*) FROM session_events
			  WHERE workspace_id='default' AND session_id=$1
			    AND type='agent.thread_message_received'
			    AND payload_json::jsonb ->> 'delivery_id'=$2),
			(SELECT count(*) FROM session_runtime_inbox
			  WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$3)`,
		sessionID,
		delivery,
		runtimeInputID,
	).Scan(&receivedCount, &inboxCount); err != nil {
		t.Fatalf("count pre-admission completion custody: %v", err)
	}
	if receivedCount != 0 || inboxCount != 0 {
		t.Fatalf("pre-admission exhausted received/inbox rows = %d/%d; want 0/0", receivedCount, inboxCount)
	}
	if repaired, err := deliveryStore.RepairCompletionMail(context.Background(), "default", defaultRuntimeInboxRepairBatch); err != nil || repaired != 0 {
		t.Fatalf("repair pre-admission exhausted completion = %d/%v; want 0/nil", repaired, err)
	}
	assertActiveCompletionWake(t, admin, sessionID, delivery, false)
}

func seedCompletionMailSentAt(
	t *testing.T,
	db *sql.DB,
	sessionID string,
	targetThreadID string,
	sourceThreadID string,
	deliveryID string,
	sequence int64,
	createdAt string,
) {
	t.Helper()
	eventID := "evt_" + deliveryID
	messageJSON := bridgeRuntimeNotificationMessageJSON(
		t,
		sessionID,
		"msg_"+deliveryID,
		completionMailEnvelope("main", "sender", deliveryID),
	)
	seedBridgeAPIEvent(
		t,
		db,
		"default",
		sessionID,
		sourceThreadID,
		eventID,
		sequence,
		"agent.thread_message_sent",
		bridgeInterAgentSentEventJSON(
			t,
			deliveryID,
			sourceThreadID,
			targetThreadID,
			"",
			"sevt_"+deliveryID,
			messageJSON,
		),
	)
	if _, err := db.ExecContext(context.Background(),
		`UPDATE session_events
		    SET created_at = $3,
		        updated_at = $3
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND event_id = $2`,
		sessionID,
		eventID,
		createdAt,
	); err != nil {
		t.Fatalf("set completion mail creation time: %v", err)
	}
}

func completionMailQueuePayload(
	t *testing.T,
	sessionID string,
	targetThreadID string,
	runtimeInputID string,
) []byte {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"workspace_id":      "default",
		"session_id":        sessionID,
		"session_thread_id": targetThreadID,
		"runtime_input_id":  runtimeInputID,
		"event_ids":         []string{},
		"sequence_from":     0,
		"sequence_to":       0,
		"input_kind":        "agent_mail",
	})
	if err != nil {
		t.Fatalf("marshal completion queue payload: %v", err)
	}
	return payload
}

func assertActiveCompletionWake(t *testing.T, db *sql.DB, sessionID string, deliveryID string, want bool) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM queue_jobs
		  WHERE workspace_id = 'default'
		    AND dedupe_key = $1
		    AND status IN ('pending', 'leased')`,
		queue.FormatRuntimeInputDedupeKey(
			workspace.ID("default"),
			sessionID,
			completionRuntimeInputID(deliveryID),
		),
	).Scan(&count); err != nil {
		t.Fatalf("count active completion wake: %v", err)
	}
	wantCount := 0
	if want {
		wantCount = 1
	}
	if count != wantCount {
		t.Fatalf("active wake for %s = %d; want %d", deliveryID, count, wantCount)
	}
}

func TestPostgreSQLCompletionMailWakeAcceptsEveryStaleRecipientArm(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *sql.DB, string, string)
		absent bool
	}{
		{name: "absent session", absent: true},
		{
			name: "deleted session",
			mutate: func(t *testing.T, admin *sql.DB, sessionID string, _ string) {
				t.Helper()
				if _, err := admin.ExecContext(context.Background(),
					`UPDATE sessions SET lifecycle_state='deleted' WHERE workspace_id='default' AND id=$1`,
					sessionID,
				); err != nil {
					t.Fatalf("delete session fixture: %v", err)
				}
			},
		},
		{
			name: "terminated session",
			mutate: func(t *testing.T, admin *sql.DB, sessionID string, _ string) {
				t.Helper()
				if _, err := admin.ExecContext(context.Background(),
					`UPDATE sessions SET status='terminated' WHERE workspace_id='default' AND id=$1`,
					sessionID,
				); err != nil {
					t.Fatalf("terminate session fixture: %v", err)
				}
			},
		},
		{
			name: "closed recipient",
			mutate: func(t *testing.T, admin *sql.DB, sessionID string, threadID string) {
				t.Helper()
				if _, err := admin.ExecContext(context.Background(),
					`UPDATE session_threads SET status='closed_for_runtime'
					  WHERE workspace_id='default' AND session_id=$1 AND id=$2`,
					sessionID,
					threadID,
				); err != nil {
					t.Fatalf("close recipient fixture: %v", err)
				}
			},
		},
		{
			name: "terminated recipient",
			mutate: func(t *testing.T, admin *sql.DB, sessionID string, threadID string) {
				t.Helper()
				if _, err := admin.ExecContext(context.Background(),
					`UPDATE session_threads SET status='terminated'
					  WHERE workspace_id='default' AND session_id=$1 AND id=$2`,
					sessionID,
					threadID,
				); err != nil {
					t.Fatalf("terminate recipient fixture: %v", err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			suffix := strings.ReplaceAll(test.name, " ", "_")
			sessionID := "sesn_completion_mail_stale_" + suffix
			threadID := "thrd_completion_mail_stale_" + suffix
			if test.absent {
				if _, err := admin.ExecContext(context.Background(),
					`INSERT INTO workspaces (id, type, name, created_at)
					 VALUES ('default', 'workspace', 'default', '2026-01-01T00:00:00Z')
					 ON CONFLICT (id) DO NOTHING`,
				); err != nil {
					t.Fatalf("seed absent-session workspace: %v", err)
				}
			} else {
				seedBridgeAPISession(t, admin, "default", sessionID, threadID)
				test.mutate(t, admin, sessionID, threadID)
			}
			store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
			plan, err := store.PrepareRuntimeCommand(context.Background(),
				completionMailRuntimeJob(sessionID, threadID, "agent_mail:delivery_stale_"+suffix))
			if err != nil {
				t.Fatalf("prepare stale completion-mail wake: %v", err)
			}
			if !plan.StaleAccepted || plan.Request != nil {
				t.Fatalf("stale completion-mail plan = %#v; want accepted no-op", plan)
			}
		})
	}
}

func TestPostgreSQLCompletionMailDoesNotRearmTerminatedRecipientAfterQueueAck(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_completion_mail_terminated_recipient"
		mainID    = "thrd_completion_mail_terminated_recipient_main"
		childID   = "thrd_completion_mail_terminated_recipient_child"
		delivery  = "delivery_completion_mail_terminated_recipient"
	)
	now := time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedCompletionMailSentAt(t, admin, sessionID, mainID, childID, delivery, 1, "2026-01-01T00:00:00Z")

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return now }
	if repaired, err := store.RepairCompletionMail(context.Background(), "default", defaultRuntimeInboxRepairBatch); err != nil || repaired != 1 {
		t.Fatalf("enqueue completion before recipient termination = %d/%v; want 1/nil", repaired, err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_threads
		    SET status='terminated'
		  WHERE workspace_id='default' AND session_id=$1 AND id=$2`,
		sessionID,
		mainID,
	); err != nil {
		t.Fatalf("terminate completion recipient: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID:   workspace.ID("default"),
		Kinds:         []string{queue.KindRuntimeInput},
		LeaseOwner:    "completion-terminated-recipient",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now,
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease completion for terminated recipient = %#v/%v; want one", leased, err)
	}
	job, err := DecodeRuntimeJob(queueJobProto(leased[0]))
	if err != nil {
		t.Fatalf("decode completion for terminated recipient: %v", err)
	}
	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil || !plan.StaleAccepted || plan.Request != nil {
		t.Fatalf("prepare completion for terminated recipient = %#v/%v; want stale accepted", plan, err)
	}
	if acknowledged, err := queueStore.Ack(context.Background(), queue.AckRequest{
		WorkspaceID: workspace.ID("default"),
		JobID:       leased[0].ID,
		LeaseToken:  leased[0].LeaseToken,
		Now:         now.Add(time.Second),
	}); err != nil || !acknowledged {
		t.Fatalf("ack terminated-recipient completion wake = %v/%v; want true/nil", acknowledged, err)
	}
	store.Clock = func() time.Time { return now.Add(2 * time.Second) }
	if repaired, err := store.RepairCompletionMail(context.Background(), "default", defaultRuntimeInboxRepairBatch); err != nil || repaired != 0 {
		t.Fatalf("repair terminated-recipient completion = %d/%v; want 0/nil", repaired, err)
	}
	assertActiveCompletionWake(t, admin, sessionID, delivery, false)
}

func TestPostgreSQLCompletionMailFinalizationRechecksTerminalRecipientFences(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *sql.DB, string, string)
	}{
		{
			name: "session terminates after prepare",
			mutate: func(t *testing.T, admin *sql.DB, sessionID string, _ string) {
				t.Helper()
				if _, err := admin.ExecContext(context.Background(),
					`UPDATE sessions SET status='terminated' WHERE workspace_id='default' AND id=$1`,
					sessionID,
				); err != nil {
					t.Fatalf("terminate session after prepare: %v", err)
				}
			},
		},
		{
			name: "recipient closes after prepare",
			mutate: func(t *testing.T, admin *sql.DB, sessionID string, threadID string) {
				t.Helper()
				if _, err := admin.ExecContext(context.Background(),
					`UPDATE session_threads SET status='closed_for_runtime'
					  WHERE workspace_id='default' AND session_id=$1 AND id=$2`,
					sessionID,
					threadID,
				); err != nil {
					t.Fatalf("close recipient after prepare: %v", err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			suffix := strings.ReplaceAll(test.name, " ", "_")
			sessionID := "sesn_completion_mail_finalize_stale_" + suffix
			threadID := "thrd_completion_mail_finalize_stale_" + suffix
			childID := "thrd_completion_mail_finalize_stale_child_" + suffix
			deliveryID := "delivery_completion_mail_finalize_stale_" + suffix
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIChildThread(t, admin, "default", sessionID, threadID, childID)
			messageJSON := bridgeRuntimeNotificationMessageJSON(
				t,
				sessionID,
				"msg_completion_mail_finalize_stale_"+suffix,
				completionMailEnvelope("main", "task_"+childID, "completion"),
			)
			seedBridgeAPIEvent(t, admin, "default", sessionID, childID, "evt_completion_mail_finalize_stale_sent_"+suffix, 1,
				"agent.thread_message_sent",
				bridgeInterAgentSentEventJSON(t, deliveryID, childID, threadID, "", "sevt_completion_mail_finalize_stale_"+suffix, messageJSON))
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_"+suffix, 1, "pod_"+suffix)
			store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
			store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
			job := completionMailRuntimeJob(
				sessionID,
				threadID,
				"agent_mail:"+deliveryID,
			)

			plan, err := store.PrepareRuntimeCommand(context.Background(), job)
			if err != nil || plan.Request == nil || plan.StaleAccepted {
				t.Fatalf("prepare completion-mail race fixture = %#v/%v; want live request", plan, err)
			}
			test.mutate(t, admin, sessionID, threadID)

			for attempt := 0; attempt < 2; attempt++ {
				finalized, err := store.FinalizeRuntimeDelivery(context.Background(), job, retryableExhaustionResult())
				if err != nil || finalized.Status != RuntimeDeliveryAccepted {
					t.Fatalf("finalize stale completion mail attempt %d = %#v/%v; want accepted no-op", attempt, finalized, err)
				}
			}
			var errorEvents int
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM session_events
				  WHERE workspace_id='default' AND session_id=$1 AND type='session.error'`,
				sessionID,
			).Scan(&errorEvents); err != nil {
				t.Fatalf("count stale finalization errors: %v", err)
			}
			if errorEvents != 0 {
				t.Fatalf("stale completion-mail finalization errors = %d; want 0", errorEvents)
			}
			if repaired, err := store.RepairRuntimeInbox(context.Background(), "default", defaultRuntimeInboxRepairBatch); err != nil || repaired != 0 {
				t.Fatalf("repair stale completion-mail inbox = %d/%v; want 0/nil", repaired, err)
			}
			assertActiveCompletionWake(t, admin, sessionID, deliveryID, false)
		})
	}
}

func TestPostgreSQLAgentMailPrepareLocksSessionBeforeInbox(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_agent_mail_lock_order"
		mainID    = "thrd_agent_mail_lock_order_main"
		childID   = "thrd_agent_mail_lock_order_child"
		bindingID = "bind_agent_mail_lock_order"
		podUID    = "pod_agent_mail_lock_order"
		delivery  = "delivery_agent_mail_lock_order"
	)
	now := time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedCompletionMailSentAt(t, admin, sessionID, mainID, childID, delivery, 1, "2026-01-01T00:00:00Z")
	apiStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	apiStore.Clock = func() time.Time { return now }
	apiStore.RuntimeBindingTokenHMACKey = []byte("agent-mail-lock-order-key-32b")
	if _, err := apiStore.ResolveInterAgentDelivery(context.Background(), &bridgev1.ResolveInterAgentDeliveryRequest{
		Scope:         bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID),
		ChildThreadId: childID,
		DeliveryId:    delivery,
	}); err != nil {
		t.Fatalf("resolve lock-order mail: %v", err)
	}

	blocker, blockerPID := lockPostgreSQLFinalizationFence(t, admin,
		`SELECT id FROM sessions WHERE workspace_id='default' AND id=$1 FOR UPDATE`,
		sessionID,
	)
	defer func() { _ = blocker.Rollback() }()
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return now }
	type prepareResult struct {
		plan RuntimeCommandPlan
		err  error
	}
	results := make(chan prepareResult, 1)
	go func() {
		plan, err := store.PrepareRuntimeCommand(context.Background(), completionMailRuntimeJob(
			sessionID,
			mainID,
			completionRuntimeInputID(delivery),
		))
		results <- prepareResult{plan: plan, err: err}
	}()
	waitForPostgreSQLLockWaiters(t, admin, blockerPID, 1)
	var lockedInputID string
	probeErr := admin.QueryRowContext(context.Background(),
		`SELECT runtime_input_id
		   FROM session_runtime_inbox
		  WHERE workspace_id='default' AND runtime_input_id=$1
		  FOR UPDATE NOWAIT`,
		completionRuntimeInputID(delivery),
	).Scan(&lockedInputID)
	if err := blocker.Rollback(); err != nil {
		t.Fatalf("release session lock-order fence: %v", err)
	}
	result := <-results
	if probeErr != nil {
		t.Fatalf("agent-mail prepare locked inbox before session: %v", probeErr)
	}
	if lockedInputID != completionRuntimeInputID(delivery) {
		t.Fatalf("lock-order probe input = %q; want %q", lockedInputID, completionRuntimeInputID(delivery))
	}
	if result.err != nil || result.plan.Request == nil {
		t.Fatalf("prepare after lock-order probe = %#v/%v; want Runtime command", result.plan, result.err)
	}
}

func completionMailRuntimeJob(sessionID string, threadID string, runtimeInputID string) RuntimeJob {
	return RuntimeJob{
		JobID:           "qjob_" + runtimeInputID,
		LeaseToken:      "lease_" + runtimeInputID,
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       sessionID,
		SessionThreadID: threadID,
		RuntimeInputID:  runtimeInputID,
		InputKind:       "agent_mail",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_AGENT_MAIL,
		PayloadJSON:     "{}",
	}
}
