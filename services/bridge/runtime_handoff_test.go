package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workload"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func handoffFixture(t *testing.T) (*PostgreSQLBridgeAPIStore, *sql.DB, bridgev1.AgentRuntimeBridgeServiceClient, *bridgev1.ReleaseRuntimeBindingRequest) {
	t.Helper()
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_handoff", "thr_handoff")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_handoff", "bind_handoff", 1, "pod_handoff")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	rpc := processRegistryRPCWithStore(t, store, "pod_handoff", nil)
	return store, admin, rpc, &bridgev1.ReleaseRuntimeBindingRequest{WorkspaceId: "default", SessionId: "sesn_handoff", BindingId: "bind_handoff", BindingGeneration: 1, RuntimeProcessId: "process_pod_handoff", OperationId: "release-handoff"}
}
func acknowledgeHandoffDrain(t *testing.T, admin *sql.DB, rpc bridgev1.AgentRuntimeBridgeServiceClient, request *bridgev1.ReleaseRuntimeBindingRequest) {
	t.Helper()
	var receipt string
	if err := admin.QueryRow(`SELECT registration_receipt FROM runtime_processes WHERE runtime_process_id=$1`, request.RuntimeProcessId).Scan(&receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := rpc.ReportRuntimeProcess(context.Background(), &bridgev1.ReportRuntimeProcessRequest{RuntimeProcessId: request.RuntimeProcessId, RegistrationReceipt: receipt, Phase: bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_DRAINING}); err != nil {
		t.Fatal(err)
	}
}
func assertHandoffUnchanged(t *testing.T, admin *sql.DB) {
	t.Helper()
	var bindings, receipts, wakes int
	if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_runtime_bindings WHERE session_id='sesn_handoff'),(SELECT count(*) FROM session_runtime_handoffs WHERE session_id='sesn_handoff'),(SELECT count(*) FROM queue_jobs WHERE kind='runtime_recovery')`).Scan(&bindings, &receipts, &wakes); err != nil {
		t.Fatal(err)
	}
	if bindings != 1 || receipts != 0 || wakes != 0 {
		t.Fatalf("rejected release changed state: binding=%d receipt=%d wakes=%d", bindings, receipts, wakes)
	}
}
func TestPostgreSQLRuntimeHandoffCheckpoint(t *testing.T) {
	t.Run("draining acknowledgement required", func(t *testing.T) {
		_, admin, rpc, request := handoffFixture(t)
		if _, err := rpc.ReleaseRuntimeBinding(context.Background(), request); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("release accepting process: %v", err)
		}
		assertHandoffUnchanged(t, admin)
	})
	for _, kind := range []string{"open provider", "unowned tool", "uncommitted reviewer"} {
		t.Run(kind, func(t *testing.T) {
			_, admin, rpc, request := handoffFixture(t)
			seedBridgeAPIInternalReviewerThread(t, admin, "default", "sesn_handoff", "thr_handoff", "thr_review")
			switch kind {
			case "open provider":
				seedBridgeAPIEvent(t, admin, "default", "sesn_handoff", "thr_review", "evt_open", 1, "span.model_request_start", "{}")
				if _, err := admin.Exec(`UPDATE session_events SET model_request_id='request-open' WHERE event_id='evt_open'`); err != nil {
					t.Fatal(err)
				}
			case "unowned tool":
				seedBridgeAPIEvent(t, admin, "default", "sesn_handoff", "thr_review", "evt_tool", 1, "agent.tool_use", "{}")
			case "uncommitted reviewer":
				if _, err := admin.Exec(`INSERT INTO session_runtime_inbox(workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,status,binding_id,binding_generation,target_pod_uid,created_at,updated_at) VALUES('default','sesn_handoff','thr_review','review-input','approval_review','accepted','bind_handoff',1,'pod_handoff',clock_timestamp(),clock_timestamp())`); err != nil {
					t.Fatal(err)
				}
			}
			acknowledgeHandoffDrain(t, admin, rpc, request)
			if _, err := rpc.ReleaseRuntimeBinding(context.Background(), request); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("checkpoint %s accepted: %v", kind, err)
			}
			assertHandoffUnchanged(t, admin)
		})
	}
	t.Run("all threads immutable continuation", func(t *testing.T) {
		_, admin, rpc, request := handoffFixture(t)
		seedBridgeAPIChildThread(t, admin, "default", "sesn_handoff", "thr_handoff", "thr_child")
		if _, err := admin.Exec(`UPDATE session_threads SET status='running' WHERE id='thr_child'`); err != nil {
			t.Fatal(err)
		}
		acknowledgeHandoffDrain(t, admin, rpc, request)
		first, err := rpc.ReleaseRuntimeBinding(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if len(first.Threads) != 2 || first.Threads[0].SessionThreadId != "thr_child" || first.Threads[0].Disposition != bridgev1.RuntimeHandoffDisposition_RUNTIME_HANDOFF_DISPOSITION_RECOVER || first.Threads[0].QueueJobId == "" || first.Threads[1].Disposition != bridgev1.RuntimeHandoffDisposition_RUNTIME_HANDOFF_DISPOSITION_IDLE || first.Threads[1].QueueJobId != "" {
			t.Fatalf("all-thread dispositions: %v", first)
		}
		var payload, kind, dedupe string
		if err := admin.QueryRow(`SELECT payload_json,kind,dedupe_key FROM queue_jobs WHERE id=$1`, first.Threads[0].QueueJobId).Scan(&payload, &kind, &dedupe); err != nil {
			t.Fatal(err)
		}
		if kind != queue.KindRuntimeRecovery || dedupe != queue.FormatRuntimeHandoffDedupeKey("default", "sesn_handoff", "thr_child", first.HandoffId) {
			t.Fatalf("handoff Queue identity %s/%s/%s", kind, dedupe, payload)
		}
		replay, err := rpc.ReleaseRuntimeBinding(context.Background(), request)
		if err != nil || !proto.Equal(first, replay) {
			t.Fatalf("receipt replay=%v error=%v", replay, err)
		}
	})
}
func TestPostgreSQLRuntimeHandoffResponseLossReplacementReplay(t *testing.T) {
	store, admin, _, request := handoffFixture(t)
	committed := make(chan *bridgev1.ReleaseRuntimeBindingResponse, 1)
	rpc := processRegistryRPCWithStore(t, store, "pod_handoff", func(ctx context.Context, method string, response any) error {
		if method == bridgev1.AgentRuntimeBridgeService_ReleaseRuntimeBinding_FullMethodName {
			committed <- proto.Clone(response.(*bridgev1.ReleaseRuntimeBindingResponse)).(*bridgev1.ReleaseRuntimeBindingResponse)
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	acknowledgeHandoffDrain(t, admin, rpc, request)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := rpc.ReleaseRuntimeBinding(ctx, request); done <- err }()
	var original *bridgev1.ReleaseRuntimeBindingResponse
	select {
	case original = <-committed:
	case <-time.After(5 * time.Second):
		t.Fatal("release commit not reached")
	}
	var persisted string
	var bindings int
	if err := admin.QueryRow(`SELECT handoff_id,(SELECT count(*) FROM session_runtime_bindings WHERE session_id='sesn_handoff') FROM session_runtime_handoffs WHERE operation_id=$1`, request.OperationId).Scan(&persisted, &bindings); err != nil || persisted != original.HandoffId || bindings != 0 {
		t.Fatalf("independent committed receipt=%s bindings=%d error=%v", persisted, bindings, err)
	}
	cancel()
	if err := <-done; status.Code(err) != codes.Canceled {
		t.Fatalf("lost ACK status: %v", err)
	}
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_handoff", "bind_replacement", 2, "pod_replacement")
	other := processRegistryRPCWithStore(t, store, "pod_handoff", nil)
	replay, err := other.ReleaseRuntimeBinding(context.Background(), request)
	if err != nil || !proto.Equal(original, replay) {
		t.Fatalf("replacement receipt replay=%v error=%v", replay, err)
	}
	for _, change := range []func(*bridgev1.ReleaseRuntimeBindingRequest){func(r *bridgev1.ReleaseRuntimeBindingRequest) { r.RuntimeProcessId = "different-boot" }, func(r *bridgev1.ReleaseRuntimeBindingRequest) { r.BindingGeneration++ }, func(r *bridgev1.ReleaseRuntimeBindingRequest) { r.BindingId = "wrong-binding" }} {
		conflicting := proto.Clone(request).(*bridgev1.ReleaseRuntimeBindingRequest)
		change(conflicting)
		if _, err := other.ReleaseRuntimeBinding(context.Background(), conflicting); status.Code(err) != codes.AlreadyExists {
			t.Fatalf("mismatched release proof: %v", err)
		}
	}
	var bindingID string
	if err := admin.QueryRow(`SELECT binding_id FROM session_runtime_bindings WHERE session_id='sesn_handoff'`).Scan(&bindingID); err != nil || bindingID != "bind_replacement" {
		t.Fatalf("replay changed replacement: %s %v", bindingID, err)
	}
}

func TestPostgreSQLRuntimeRetiredProcessReceiptFence(t *testing.T) {
	store, admin, rpc, _ := handoffFixture(t)
	scope := bridgeAPIScope("sesn_handoff", "thr_handoff", "bind_handoff", 1, "pod_handoff")
	request := &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: "write-running", EventType: "session.status_running", PayloadJson: `{"type":"session.status_running"}`}
	first, err := store.WriteEvent(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := rpc.RegisterRuntimeProcess(context.Background(), &bridgev1.RegisterRuntimeProcessRequest{RuntimeProcessId: "new-boot"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rpc.ReportRuntimeProcess(context.Background(), &bridgev1.ReportRuntimeProcessRequest{RuntimeProcessId: candidate.RuntimeProcessId, RegistrationReceipt: candidate.RegistrationReceipt, Phase: bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_ACCEPTING}); err != nil {
		t.Fatal(err)
	}
	replay, err := store.WriteEvent(context.Background(), request)
	if err != nil || replay.GetDuplicate() == nil || first.GetCommitted().GetEventId() != replay.GetDuplicate().GetEventId() {
		t.Fatalf("exact retired receipt=%v error=%v", replay, err)
	}
	unseen := proto.Clone(request).(*bridgev1.WriteEventRequest)
	unseen.RuntimeWriteId = "write-unseen"
	if _, err := store.WriteEvent(context.Background(), unseen); !runtimecontrol.IsScopeSupersededError(err) {
		t.Fatalf("retired new mutation=%v", err)
	}
	if _, err := admin.Exec(`UPDATE session_runtime_bindings SET binding_id='new-binding',binding_generation=2,runtime_process_id='new-boot' WHERE session_id='sesn_handoff'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.WriteEvent(context.Background(), request); !runtimecontrol.IsScopeSupersededError(err) {
		t.Fatalf("receipt crossed replacement fence=%v", err)
	}
}

func TestPostgreSQLRuntimeHandoffPreservesSandboxExecutionCustody(t *testing.T) {
	for _, state := range []string{"pending", "preparing", "running", "waiting_activation", "waiting_materialization", "terminal_unconsumed"} {
		t.Run(state, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			scope, tool := seedAwaitExecutionNotificationFixture(t, store, admin, "handoff_"+state)
			var messageSequence int64
			if err := admin.QueryRow(`SELECT sequence FROM session_messages WHERE workspace_id=$1 AND session_id=$2 AND model_request_id=$3`, scope.WorkspaceId, scope.SessionId, "mreq_exec_notify_handoff_"+state).Scan(&messageSequence); err != nil {
				t.Fatal(err)
			}
			if _, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{Scope: scope, RuntimeWriteId: "end_handoff_" + state, ModelRequestId: "mreq_exec_notify_handoff_" + state, FinishReason: "tool-calls", UsageJson: `{}`, ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "completed", AssistantMessageSequence: &messageSequence, ToolUseEventIds: []string{tool}}}); err != nil {
				t.Fatal(err)
			}
			if state == "terminal_unconsumed" {
				commitAwaitExecutionSettlement(t, admin, scope, tool, `{"status":"success","result":{"stdout":"original"}}`, false)
			} else if _, err := admin.Exec(`UPDATE session_runtime_tool_results SET execution_state=$1 WHERE tool_use_event_id=$2`, state, tool); err != nil {
				t.Fatal(err)
			}
			var executionBefore, jobsBefore string
			read := func() (string, string) {
				t.Helper()
				var execution, jobs string
				if err := admin.QueryRow(`SELECT to_jsonb(result)::text FROM session_runtime_tool_results result WHERE tool_use_event_id=$1`, tool).Scan(&execution); err != nil {
					t.Fatal(err)
				}
				if err := admin.QueryRow(`SELECT jsonb_agg(to_jsonb(job) ORDER BY id)::text FROM queue_jobs job WHERE kind='sandbox_tool_execute'`).Scan(&jobs); err != nil {
					t.Fatal(err)
				}
				return execution, jobs
			}
			executionBefore, jobsBefore = read()
			request := &bridgev1.ReleaseRuntimeBindingRequest{WorkspaceId: scope.WorkspaceId, SessionId: scope.SessionId, BindingId: scope.Binding.BindingId, BindingGeneration: scope.Binding.BindingGeneration, RuntimeProcessId: scope.Binding.RuntimeProcessId, OperationId: "release_" + state}
			rpc := processRegistryRPCWithStore(t, store, scope.Binding.TargetPodUid, nil)
			acknowledgeHandoffDrain(t, admin, rpc, request)
			response, err := rpc.ReleaseRuntimeBinding(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if len(response.Threads) != 1 || response.Threads[0].Disposition != bridgev1.RuntimeHandoffDisposition_RUNTIME_HANDOFF_DISPOSITION_RECOVER || response.Threads[0].QueueJobId == "" {
				t.Fatalf("accepted execution disposition=%v", response)
			}
			executionAfter, jobsAfter := read()
			if executionBefore != executionAfter || jobsBefore != jobsAfter {
				t.Fatalf("handoff altered independent Sandbox custody: execution=%v jobs=%v", executionBefore == executionAfter, jobsBefore == jobsAfter)
			}
		})
	}
}

func TestPostgreSQLRuntimeHandoffOrdinaryInputCustody(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		t.Run(fmt.Sprintf("rollback_%t", rollback), func(t *testing.T) {
			_, admin, rpc, request := handoffFixture(t)
			q := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(admin))
			type fact struct {
				id, job, payload string
				attempts         int
			}
			var facts []fact
			for _, kind := range []string{"messages", "interrupt_control", "tool_confirmation", "task_notification", "agent_mail", "rejection"} {
				for _, custody := range []string{"accepted", "delivering"} {
					inputID := kind + ":" + custody
					threadID := "thread_" + kind + "_" + custody
					seedBridgeAPIChildThread(t, admin, "default", "sesn_handoff", "thr_handoff", threadID)
					eventIDs, _ := json.Marshal([]string{"event_" + inputID})
					input := runtimecontrol.AcceptedRuntimeInput{SessionThreadID: threadID, RuntimeInputID: inputID, InputKind: kind, EventIDsJSON: string(eventIDs), SequenceFrom: sql.NullInt64{Int64: 1, Valid: true}, SequenceTo: sql.NullInt64{Int64: 1, Valid: true}}
					enqueue, err := runtimecontrol.RuntimeInputEnqueueRequest("default", "sesn_handoff", input, time.Now())
					if err != nil {
						t.Fatal(err)
					}
					job, err := q.Enqueue(context.Background(), enqueue)
					if err != nil {
						t.Fatal(err)
					}
					// Real Queue identities are retained; set the pre-existing attempted lease
					// to distinguish transfer from creating a replacement with reset lineage.
					if _, err := admin.Exec(`UPDATE queue_jobs SET status='leased',attempt_count=3,lease_token=$2,leased_by='old-runner',leased_at=clock_timestamp(),leased_until=clock_timestamp()+interval '1 hour' WHERE id=$1`, job.ID, "old_"+inputID); err != nil {
						t.Fatal(err)
					}
					if _, err := admin.Exec(`INSERT INTO session_runtime_inbox(workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,status,event_ids_json,sequence_from,sequence_to,binding_id,binding_generation,target_pod_uid,created_at,updated_at) VALUES('default','sesn_handoff',$5,$1,$2,$3,$4,1,1,'bind_handoff',1,'pod_handoff',clock_timestamp(),clock_timestamp())`, inputID, kind, custody, string(eventIDs), threadID); err != nil {
						t.Fatal(err)
					}
					var payload string
					if err := admin.QueryRow(`SELECT payload_json FROM queue_jobs WHERE id=$1`, job.ID).Scan(&payload); err != nil {
						t.Fatal(err)
					}
					facts = append(facts, fact{inputID, job.ID, payload, 3})
				}
			}
			if _, err := admin.Exec(`INSERT INTO session_runtime_inbox(workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,status,event_ids_json,sequence_from,sequence_to,binding_id,binding_generation,target_pod_uid,committed_at,created_at,updated_at) VALUES('default','sesn_handoff','thr_handoff','committed_message','messages','committed','["committed_event"]',1,1,'bind_handoff',1,'pod_handoff',clock_timestamp(),clock_timestamp(),clock_timestamp())`); err != nil {
				t.Fatal(err)
			}
			if rollback {
				if _, err := admin.Exec(`INSERT INTO session_runtime_inbox(workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,status,event_ids_json,sequence_from,sequence_to,binding_id,binding_generation,target_pod_uid,created_at,updated_at) VALUES('default','sesn_handoff','thr_handoff','zz_missing_queue','messages','delivering','["event_missing"]',1,1,'bind_handoff',1,'pod_handoff',clock_timestamp(),clock_timestamp())`); err != nil {
					t.Fatal(err)
				}
			}
			acknowledgeHandoffDrain(t, admin, rpc, request)
			before := receiptTenantSnapshot(t, admin)
			response, err := rpc.ReleaseRuntimeBinding(context.Background(), request)
			if rollback {
				if err == nil {
					t.Fatalf("missing delivering Queue custody released: %v", response)
				}
				if !reflect.DeepEqual(before, receiptTenantSnapshot(t, admin)) {
					t.Fatal("failed transfer changed tenant data")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, f := range facts {
				var inbox, status, payload string
				var token sql.NullString
				var attempts int
				if err := admin.QueryRow(`SELECT status FROM session_runtime_inbox WHERE runtime_input_id=$1`, f.id).Scan(&inbox); err != nil {
					t.Fatal(err)
				}
				if err := admin.QueryRow(`SELECT status,payload_json,attempt_count,lease_token FROM queue_jobs WHERE id=$1`, f.job).Scan(&status, &payload, &attempts, &token); err != nil {
					t.Fatal(err)
				}
				if inbox != "queued" || status != "pending" || attempts != f.attempts || payload != f.payload || token.Valid {
					t.Fatalf("custody %s inbox=%s queue=%s attempts=%d lease=%v payload unchanged=%v", f.id, inbox, status, attempts, token, payload == f.payload)
				}
				stale, err := q.Ack(context.Background(), queue.AckRequest{WorkspaceID: "default", JobID: f.job, LeaseToken: "old_" + f.id})
				if err != nil || stale {
					t.Fatalf("old Queue capability %s=%v/%v", f.id, stale, err)
				}
			}
			var committed string
			if err := admin.QueryRow(`SELECT to_jsonb(inbox)::text FROM session_runtime_inbox inbox WHERE runtime_input_id='committed_message'`).Scan(&committed); err != nil {
				t.Fatal(err)
			}
			var beforeCommitted []map[string]any
			if err := json.Unmarshal([]byte(before["session_runtime_inbox"]), &beforeCommitted); err != nil {
				t.Fatal(err)
			}
			var afterCommitted map[string]any
			if err := json.Unmarshal([]byte(committed), &afterCommitted); err != nil {
				t.Fatal(err)
			}
			for _, row := range beforeCommitted {
				if row["runtime_input_id"] == "committed_message" && !reflect.DeepEqual(row, afterCommitted) {
					t.Fatal("committed input changed during handoff")
				}
			}
		})
	}
}

func TestPostgreSQLRuntimeHandoffDiagnosticsReachProcessSink(t *testing.T) {
	store, admin, rpc, request := handoffFixture(t)
	var buffer bytes.Buffer
	owner := workload.NewProcessLogger(&buffer, "bridge", "test", "unit", workload.DefaultDiagnosticConfig())
	defer owner.CloseWithBudget()
	store.Logger = owner.Logger
	// Both a rejected non-draining release and a real all-thread success must
	// retain the same operation/scope/process correlation at the safe process sink.
	if _, err := rpc.ReleaseRuntimeBinding(context.Background(), request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("accepting release=%v", err)
	}
	seedBridgeAPIChildThread(t, admin, "default", "sesn_handoff", "thr_handoff", "thr_diagnostic_child")
	if _, err := admin.Exec(`INSERT INTO session_runtime_inbox(workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,status,event_ids_json,sequence_from,sequence_to,binding_id,binding_generation,target_pod_uid,created_at,updated_at) VALUES('default','sesn_handoff','thr_handoff','input_diagnostic','messages','accepted','["event_diagnostic"]',1,1,'bind_handoff',1,'pod_handoff',clock_timestamp(),clock_timestamp())`); err != nil {
		t.Fatal(err)
	}
	acknowledgeHandoffDrain(t, admin, rpc, request)
	released, err := rpc.ReleaseRuntimeBinding(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	owner.CloseWithBudget()
	decoder := json.NewDecoder(&buffer)
	var failure, success map[string]any
	for decoder.More() {
		var record map[string]any
		if err := decoder.Decode(&record); err != nil {
			t.Fatal(err)
		}
		switch record["msg"] {
		case "runtime.binding.release_rejected":
			failure = record
		case "runtime.binding.released":
			success = record
		}
	}
	if failure == nil || success == nil {
		t.Fatalf("release records omitted failure=%v success=%v", failure, success)
	}
	for _, record := range []map[string]any{failure, success} {
		for key, want := range map[string]any{"service.name": "bridge", "operation": "release_runtime_binding", "operation.id": request.OperationId, "workspace.id": request.WorkspaceId, "session.id": request.SessionId, "binding.id": request.BindingId, "binding.generation": float64(request.BindingGeneration), "runtime.process.id": request.RuntimeProcessId} {
			if record[key] != want {
				t.Errorf("safe sink lost %s=%v want=%v", key, record[key], want)
			}
		}
	}
	if failure["grpc.code"] != codes.FailedPrecondition.String() {
		t.Fatalf("release failure status missing=%v", failure)
	}
	for key, want := range map[string]any{"handoff.id": released.HandoffId, "target.count": float64(2), "input.count": float64(1)} {
		if success[key] != want {
			t.Errorf("safe sink lost release %s=%v want=%v", key, success[key], want)
		}
	}
	for _, record := range []map[string]any{failure, success} {
		for _, unsupported := range []string{"rpc.code", "handoff.threads", "custody.handed_back"} {
			if _, ok := record[unsupported]; ok {
				t.Errorf("release used unsupported diagnostic alias %s", unsupported)
			}
		}
	}
}
