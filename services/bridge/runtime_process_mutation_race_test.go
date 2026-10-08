package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// Exact statement text of the single binding/process verification in
// lockRuntimeBindingProcessTx.
const (
	servingProcessLockSQL = "FROM public.tetral_lock_runtime_process($1,$2,$3)"
	servingBindingLockSQL = "FROM session_runtime_bindings WHERE workspace_id=$1 AND session_id=$2 AND binding_id=$3 AND binding_generation=$4 FOR UPDATE"
)

func waitProcessSQLLock(ctx context.Context, t *testing.T, admin *sql.DB, pattern string) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		if err := admin.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE $1)`, "%"+pattern+"%").Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("actual process SQL lock barrier not reached")
		}
	}
}

// Two independently addressable Bridge replicas use separate ordinary RLS
// pools. Barriers park real serving SQL; independent connections inspect actual
// committed effects and PostgreSQL waiters, never a direct fence-helper call.
func TestPostgreSQLRuntimeProcessServingMutationRaces(t *testing.T) {
	for _, operation := range []string{"assistant event", "tool declaration", "input commit", "request checkpoint", "tool settlement"} {
		for _, promotionFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/promotion_first_%t", operation, promotionFirst), func(t *testing.T) {
				runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
				seed := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
				scope, tool := seedAwaitExecutionNotificationFixture(t, seed, admin, "serving_fence")
				modelRequest := "mreq_exec_notify_serving_fence"
				request := &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: "write-race", ModelRequestId: modelRequest, PreallocatedEventId: bridgeString("evt_00000000000000000000000000000002"), EventType: "agent.message", PayloadJson: `{"type":"agent.message","content":[{"type":"text","text":"original"}]}`, AssistantContextDelta: sessionfixture.BridgeTextContextDeltaForTest("original")}
				if operation == "tool declaration" {
					request = &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: "write-race", ModelRequestId: modelRequest, ToolDeclaration: sessionfixture.BridgeToolDeclarationWithRouteForTest("call-race", "Read", `{"file_path":"/workspace/once"}`, "allow")}
				}
				if operation == "input commit" {
					seq := sessionfixture.NextBridgeAPIEventSequenceForTest(t, admin, scope.SessionId, scope.SessionThreadId)
					seedBridgeAPIEvent(t, admin, "default", scope.SessionId, scope.SessionThreadId, "event-race-input", seq, "user.message", `{"content":[{"type":"text","text":"input"}]}`)
					sessionfixture.SeedBridgeAPIRuntimeInbox(t, admin, "default", scope.SessionId, scope.SessionThreadId, "input-race", "messages", `["event-race-input"]`, "accepted", scope.Binding.BindingId, scope.Binding.TargetPodUid, seq, seq)
				}
				if operation == "tool settlement" {
					commitAwaitExecutionSettlement(t, admin, scope, tool, `{"status":"success","result":{"stdout":"original"}}`, false)
				}
				var sequence int64
				if err := admin.QueryRow(`SELECT sequence FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND model_request_id=$2`, scope.SessionId, modelRequest).Scan(&sequence); err != nil {
					t.Fatal(err)
				}
				writerTrace, promotionTrace := &bridgeExecutionQueryTracer{}, &bridgeExecutionQueryTracer{}
				writer := processRegistryRPCWithStore(t, newAwaitNotificationTracedStore(t, runtime, writerTrace), scope.Binding.TargetPodUid, nil)
				promoter := processRegistryRPCWithStore(t, newAwaitNotificationTracedStore(t, runtime, promotionTrace), scope.Binding.TargetPodUid, nil)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				next, err := promoter.RegisterRuntimeProcess(ctx, &bridgev1.RegisterRuntimeProcessRequest{RuntimeProcessId: "replacement-serving"})
				if err != nil {
					t.Fatal(err)
				}
				type mutationOutcome struct {
					stale bool
					err   error
				}
				mutate := func() mutationOutcome {
					switch operation {
					case "input commit":
						response, err := writer.CommitInputs(ctx, &bridgev1.CommitInputsRequest{Scope: scope, RuntimeInputId: "input-race"})
						return mutationOutcome{response.GetStale() != nil, err}
					case "request checkpoint":
						response, err := writer.WriteRequestEnd(ctx, &bridgev1.WriteRequestEndRequest{Scope: scope, RuntimeWriteId: "end-race", ModelRequestId: modelRequest, FinishReason: "tool-calls", UsageJson: `{}`, ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "completed", AssistantMessageSequence: &sequence, ToolUseEventIds: []string{tool}}})
						return mutationOutcome{response.GetStale() != nil, err}
					case "tool settlement":
						response, err := writer.SettleToolResult(ctx, sessionfixture.BridgeToolSettlementRequestForTest(scope, sessionfixture.BridgeCompletedToolSettlementForTest(tool, "original")))
						return mutationOutcome{response.GetStale() != nil, err}
					default:
						response, err := writer.WriteEvent(ctx, request)
						return mutationOutcome{response.GetStale() != nil, err}
					}
				}
				promote := func() error {
					_, err := promoter.ReportRuntimeProcess(ctx, &bridgev1.ReportRuntimeProcessRequest{RuntimeProcessId: next.RuntimeProcessId, RegistrationReceipt: next.RegistrationReceipt, Phase: bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_ACCEPTING})
					return err
				}
				var fired, release chan struct{}
				if promotionFirst {
					fired, release = promotionTrace.armBarrier("", "UPDATE runtime_process_pods SET last_promoted_order", 1)
				} else {
					// The receipt-scope proof's process lock is the only one the
					// mutation takes; the writer parks while holding it.
					fired, release = writerTrace.armBarrier("", servingProcessLockSQL, 1)
				}
				var once sync.Once
				resume := func() { once.Do(func() { close(release) }) }
				defer resume()
				before := receiptTenantSnapshot(t, admin)
				var eventsBefore, receiptsBefore int
				if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_events WHERE session_id=$1),(SELECT count(*) FROM session_bridge_operations WHERE session_id=$1)`, scope.SessionId).Scan(&eventsBefore, &receiptsBefore); err != nil {
					t.Fatal(err)
				}
				written, promoted := make(chan mutationOutcome, 1), make(chan error, 1)
				if promotionFirst {
					go func() { promoted <- promote() }()
				} else {
					go func() { written <- mutate() }()
				}
				select {
				case <-fired:
				case <-ctx.Done():
					t.Fatal("actual serving authority barrier not reached")
				}
				if promotionFirst {
					go func() { written <- mutate() }()
					waitProcessSQLLock(ctx, t, admin, "tetral_lock_runtime_process")
				} else {
					go func() { promoted <- promote() }()
					waitProcessSQLLock(ctx, t, admin, "ORDER BY registration_order FOR UPDATE")
				}
				if after := receiptTenantSnapshot(t, admin); !reflect.DeepEqual(before, after) {
					t.Fatal("uncommitted serving effect visible before barrier release")
				}
				resume()
				if err := <-promoted; err != nil {
					t.Fatalf("actual promotion=%v", err)
				}
				writeResult := <-written
				writeErr := writeResult.err
				if locks, bindings := writerTrace.countSQL(servingProcessLockSQL), writerTrace.countSQL(servingBindingLockSQL); locks != 1 || bindings != 1 {
					t.Fatalf("%s took %d process and %d binding locks; want one each", operation, locks, bindings)
				}
				after := receiptTenantSnapshot(t, admin)
				if promotionFirst {
					if !writeResult.stale && status.Code(writeErr) != codes.FailedPrecondition {
						t.Fatalf("old actual %s mutation not fenced:%v", operation, writeErr)
					}
					if !reflect.DeepEqual(before, after) {
						for table, rows := range before {
							if after[table] != rows {
								t.Errorf("retired actual mutation changed%s", table)
							}
						}
					}
				} else {
					if writeErr != nil {
						t.Fatalf("admitted current mutation failed:%v", writeErr)
					}
					if reflect.DeepEqual(before, after) {
						t.Fatal("actual admitted mutation committed no effects")
					}
					if before["session_bridge_operations"] == after["session_bridge_operations"] {
						t.Fatal("committed mutation omitted durable receipt")
					}
					assertServingMutationEffects(t, admin, scope, operation, modelRequest, tool, sequence, eventsBefore, receiptsBefore)
					// Replay is pure under the unchanged binding even after process promotion.
					if replay := mutate(); replay.err != nil || replay.stale {
						t.Fatalf("exact retired ordinary receipt=%+v", replay)
					}
					if replay := receiptTenantSnapshot(t, admin); !reflect.DeepEqual(after, replay) {
						t.Fatal("exact retired receipt replay mutated tenant rows")
					}
				}
				var current string
				if err := admin.QueryRow(`SELECT runtime_process_id FROM runtime_processes WHERE pod_uid=$1 AND is_current`, scope.Binding.TargetPodUid).Scan(&current); err != nil || current != next.RuntimeProcessId {
					t.Fatalf("current process=%s/%v", current, err)
				}
				// Independent reconciliation progresses after each barrier, then new current
				// scope writes proceed; the old receipt no longer crosses the binding cut.
				if _, err := admin.Exec(`UPDATE session_runtime_bindings SET runtime_process_id=$1,binding_id='replacement-binding',binding_generation=2 WHERE workspace_id='default' AND session_id=$2`, next.RuntimeProcessId, scope.SessionId); err != nil {
					t.Fatal(err)
				}
				cutSnapshot := receiptTenantSnapshot(t, admin)
				if retired := mutate(); retired.err == nil && !retired.stale {
					t.Fatalf("ordinary old receipt crossed reconciled binding: %+v", retired)
				}
				if afterCut := receiptTenantSnapshot(t, admin); !reflect.DeepEqual(cutSnapshot, afterCut) {
					t.Fatal("retired scope mutated after binding reconciliation")
				}
				scope.Binding.RuntimeProcessId = next.RuntimeProcessId
				scope.Binding.BindingId = "replacement-binding"
				scope.Binding.BindingGeneration = 2
				nextWrite := &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: "current-after-fence", EventType: "session.status_running", PayloadJson: `{"type":"session.status_running"}`}
				if response, err := promoter.WriteEvent(ctx, nextWrite); err != nil || response.GetCommitted() == nil {
					t.Fatalf("current mutation stalled after barrier:%v/%v", response, err)
				}
			})
		}
	}
}

type registerBeforeLockTracer struct {
	reached, release chan struct{}
	once             sync.Once
}

func (tr *registerBeforeLockTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "INSERT INTO runtime_process_pods") {
		tr.once.Do(func() { close(tr.reached); <-tr.release })
	}
	return ctx
}
func (*registerBeforeLockTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}
func TestPostgreSQLRuntimeUnseenRegistrationCrossesPromotion(t *testing.T) {
	if address := os.Getenv("TETRAL_REGISTER_PROCESS_CHILD"); address != "" {
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		if _, err := bridgev1.NewAgentRuntimeBridgeServiceClient(conn).RegisterRuntimeProcess(context.Background(), &bridgev1.RegisterRuntimeProcessRequest{RuntimeProcessId: "unseen-delayed-boot"}); err != nil {
			t.Fatal(err)
		}
		t.Fatal("killed process unexpectedly received registration receipt")
		return
	}

	for _, promotionFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("promotion_first_%t", promotionFirst), func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			scope, _ := seedAwaitExecutionNotificationFixture(t, store, admin, "unseen_register")
			oldProcess := scope.Binding.RuntimeProcessId
			var oldReceipt string
			if err := admin.QueryRow(`SELECT registration_receipt FROM runtime_processes WHERE runtime_process_id=$1`, oldProcess).Scan(&oldReceipt); err != nil {
				t.Fatal(err)
			}
			tracer := &registerBeforeLockTracer{reached: make(chan struct{}), release: make(chan struct{})}
			pool := storagetest.OpenRuntimeRoleDBWithTracer(t, runtime, tracer)
			_, address := processRegistryRPCWithIdentityAddress(t, NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(pool)), auth.Identity{ServiceAccount: auth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"}, KubernetesPodUID: scope.Binding.TargetPodUid}, nil)
			proxyAddress, resumeDisconnect := holdRegistrationDisconnect(t, address)
			defer resumeDisconnect()
			winner := processRegistryRPC(t, dbconnect.NewClientForTesting(runtime), scope.Binding.TargetPodUid)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			next, err := winner.RegisterRuntimeProcess(ctx, &bridgev1.RegisterRuntimeProcessRequest{RuntimeProcessId: "live-register-winner"})
			if err != nil {
				t.Fatal(err)
			}
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPostgreSQLRuntimeUnseenRegistrationCrossesPromotion$")
			child.Env = append(os.Environ(), "TETRAL_REGISTER_PROCESS_CHILD="+proxyAddress)
			var output bytes.Buffer
			child.Stdout = &output
			child.Stderr = &output
			if err := child.Start(); err != nil {
				t.Fatal(err)
			}
			killed := false
			defer func() {
				if !killed {
					_ = child.Process.Kill()
					_ = child.Wait()
				}
			}()

			select {
			case <-tracer.reached:
			case <-ctx.Done():
				close(tracer.release)
				t.Fatal("unseen Register did not stop before its own row lock")
			}
			if err := child.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			childErr := child.Wait()
			killed = true
			if childErr == nil || child.ProcessState.Success() {
				t.Fatal("P1 did not actually die before registration lock")
			}
			t.Logf("killed actual registering process pid=%d before own row lock; transparent proxy delays EOF", child.Process.Pid)
			promote := func() {
				if _, err := winner.ReportRuntimeProcess(ctx, &bridgev1.ReportRuntimeProcessRequest{RuntimeProcessId: next.RuntimeProcessId, RegistrationReceipt: next.RegistrationReceipt, Phase: bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_ACCEPTING}); err != nil {
					t.Fatal(err)
				}
				if _, err := admin.Exec(`UPDATE session_runtime_bindings SET runtime_process_id=$1 WHERE workspace_id='default' AND session_id=$2`, next.RuntimeProcessId, scope.SessionId); err != nil {
					t.Fatal(err)
				}
				scope.Binding.RuntimeProcessId = next.RuntimeProcessId
				if response, err := winner.WriteEvent(ctx, &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: "original-binding-before-late", EventType: "session.status_running", PayloadJson: `{"type":"session.status_running"}`}); err != nil || response.GetCommitted() == nil {
					t.Fatalf("P2 original binding failed before late allocation:%v/%v", response, err)
				}
			}
			if promotionFirst {
				promote()
			}
			tenantBefore := receiptTenantSnapshot(t, admin)
			close(tracer.release)
			var lateOrder int64
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			for {
				err := admin.QueryRow(`SELECT registration_order FROM runtime_processes WHERE runtime_process_id='unseen-delayed-boot'`).Scan(&lateOrder)
				if err == nil {
					break
				}
				if err != sql.ErrNoRows {
					t.Fatal(err)
				}
				select {
				case <-tick.C:
				case <-ctx.Done():
					t.Fatal("admitted late Register did not commit after killed client")
				}
			}
			if lateOrder <= next.RegistrationOrder {
				t.Fatal("delayed unseen allocation order not independently observed")
			}
			if after := receiptTenantSnapshot(t, admin); !reflect.DeepEqual(tenantBefore, after) {
				t.Fatal("late Register/promotion changed binding, Queue, checkpoints or external dispatch custody")
			}

			if !promotionFirst {
				promote()
			}
			var current string
			var lateCurrent bool
			if err := admin.QueryRow(`SELECT runtime_process_id FROM runtime_processes WHERE pod_uid=$1 AND is_current`, scope.Binding.TargetPodUid).Scan(&current); err != nil || current != next.RuntimeProcessId {
				t.Fatalf("unconfirmed higher candidate displaced/blocked live winner:%s/%v", current, err)
			}
			if err := admin.QueryRow(`SELECT is_current FROM runtime_processes WHERE runtime_process_id=$1`, "unseen-delayed-boot").Scan(&lateCurrent); err != nil || lateCurrent {
				t.Fatal("unconfirmed delayed boot became current")
			}
			bindingBefore := receiptTenantSnapshot(t, admin)["session_runtime_bindings"]
			if response, err := winner.WriteEvent(ctx, &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: "surviving-original-binding", PreallocatedEventId: bridgeString("evt_00000000000000000000000000000003"), EventType: "agent.message", ModelRequestId: "mreq_exec_notify_unseen_register", PayloadJson: `{"type":"agent.message","content":[{"type":"text","text":"survivor"}]}`, AssistantContextDelta: sessionfixture.BridgeTextContextDeltaForTest("survivor")}); err != nil || response.GetCommitted() == nil {
				t.Fatalf("P2 original binding did not keep writing:%v/%v", response, err)
			}
			afterWrite := receiptTenantSnapshot(t, admin)
			if afterWrite["session_runtime_bindings"] != bindingBefore || afterWrite["queue_jobs"] != tenantBefore["queue_jobs"] {
				t.Fatal("surviving write invented loss repair/binding or Queue external dispatch")
			}
			var lost int
			if err := admin.QueryRow(`SELECT count(*) FROM session_events WHERE session_id=$1 AND (type='session.error' OR type='span.model_request_end')`, scope.SessionId).Scan(&lost); err != nil || lost != 0 {
				t.Fatalf("late registration invented request-end/loss:%d/%v", lost, err)
			}
			resumeDisconnect()
			if _, err := winner.ReportRuntimeProcess(ctx, &bridgev1.ReportRuntimeProcessRequest{RuntimeProcessId: oldProcess, RegistrationReceipt: oldReceipt, Phase: bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_ACCEPTING}); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("old accepting report reentered after promotion:%v", err)
			}
		})
	}
}

// Independent SQL oracles identify the precise committed projection, rather
// than treating an arbitrary changed row or successful helper as evidence.
func assertServingMutationEffects(t *testing.T, admin *sql.DB, scope *bridgev1.RuntimeScope, operation, modelRequest, tool string, sequence int64, eventsBefore, receiptsBefore int) {
	t.Helper()
	var events, receipts int
	if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_events WHERE session_id=$1),(SELECT count(*) FROM session_bridge_operations WHERE session_id=$1)`, scope.SessionId).Scan(&events, &receipts); err != nil {
		t.Fatal(err)
	}
	expectedEvents := eventsBefore + 1
	if operation == "input commit" {
		expectedEvents = eventsBefore
	}
	if events != expectedEvents || receipts != receiptsBefore+1 {
		t.Fatalf("exact %s event/receipt deltas=%d/%d; want %d/1", operation, events-eventsBefore, receipts-receiptsBefore, expectedEvents-eventsBefore)
	}
	var correct bool
	var query string
	var args []any
	switch operation {
	case "assistant event":
		query = `SELECT EXISTS(SELECT 1 FROM session_events e JOIN session_messages m ON m.session_id=e.session_id AND m.model_request_id=e.model_request_id WHERE e.session_id=$1 AND e.runtime_write_id='write-race' AND e.type='agent.message' AND e.payload_json::jsonb #>> '{content,0,text}'='original' AND m.kind='assistant' AND m.data_json::jsonb @> '{"parts":[{"type":"text","text":"original"}]}')`
		args = []any{scope.SessionId}
	case "tool declaration":
		query = `SELECT EXISTS(SELECT 1 FROM session_events e JOIN session_messages m ON m.session_id=e.session_id AND m.model_request_id=e.model_request_id WHERE e.session_id=$1 AND e.runtime_write_id='write-race' AND e.type='agent.tool_use' AND e.projection_json::jsonb->>'model_tool_call_id'='call-race' AND e.projection_json::jsonb->>'tool_name'='Read' AND m.data_json LIKE '%call-race%' AND m.data_json LIKE '%/workspace/once%')`
		args = []any{scope.SessionId}
	case "input commit":
		query = `SELECT EXISTS(SELECT 1 FROM session_runtime_inbox i JOIN session_messages m ON m.session_id=i.session_id AND m.source_event_id='event-race-input' WHERE i.session_id=$1 AND i.runtime_input_id='input-race' AND i.status='committed' AND i.committed_at IS NOT NULL AND m.kind='user' AND m.data_json::jsonb @> '{"parts":[{"type":"text","text":"input"}]}')`
		args = []any{scope.SessionId}
	case "request checkpoint":
		query = `SELECT EXISTS(SELECT 1 FROM session_events e JOIN session_messages m ON m.session_id=e.session_id AND m.model_request_id=e.model_request_id WHERE e.session_id=$1 AND e.runtime_write_id='end-race' AND e.type='span.model_request_end' AND e.payload_json::jsonb #>> '{provider_context_retention,disposition}'='completed' AND e.payload_json::jsonb #>> '{provider_context_retention,assistant_message_sequence}'=$2 AND e.payload_json::jsonb #> '{provider_context_retention,tool_use_event_ids}'=jsonb_build_array($3::text) AND m.sequence=$2::bigint AND m.model_request_id=$4)`
		args = []any{scope.SessionId, fmt.Sprint(sequence), tool, modelRequest}
	case "tool settlement":
		query = `SELECT EXISTS(SELECT 1 FROM session_events e JOIN session_messages m ON m.session_id=e.session_id AND m.model_request_id=e.model_request_id JOIN session_runtime_tool_results r ON r.session_id=e.session_id AND r.tool_use_event_id=$2 WHERE e.session_id=$1 AND e.type='agent.tool_result' AND e.payload_json::jsonb->>'tool_use_id'=$2 AND m.data_json LIKE '%tool_result%' AND m.data_json LIKE '%original%' AND r.execution_state='consumed')`
		args = []any{scope.SessionId, tool}
	}
	if err := admin.QueryRow(query, args...).Scan(&correct); err != nil || !correct {
		t.Fatalf("exact %s durable event/context/checkpoint/bookkeeping=%t/%v", operation, correct, err)
	}
	var invalidReceipts int
	if err := admin.QueryRow(`SELECT count(*) FROM session_bridge_operations WHERE session_id=$1 AND (ack_status<>'committed' OR request_hash='' OR result_json IS NULL)`, scope.SessionId).Scan(&invalidReceipts); err != nil || invalidReceipts != 0 {
		t.Fatalf("durable receipt facts invalid=%d/%v", invalidReceipts, err)
	}
}

// Forward bytes exactly once. Client EOF is delayed to model an admitted request
// whose process died before the network delivered cancellation to Bridge. The
// actual handler context and SQL remain untouched; no RPC is replayed.
func holdRegistrationDisconnect(t *testing.T, address string) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	delay := make(chan struct{})
	joined := make(chan struct{})
	var once sync.Once
	resume := func() { once.Do(func() { close(delay) }) }
	go func() {
		defer close(joined)
		downstream, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = downstream.Close() }()
		upstream, err := net.Dial("tcp", address)
		if err != nil {
			return
		}
		defer func() { _ = upstream.Close() }()
		copied := make(chan struct{}, 2)
		go func() { _, _ = io.Copy(upstream, downstream); copied <- struct{}{} }()
		go func() { _, _ = io.Copy(downstream, upstream); copied <- struct{}{} }()
		<-copied
		<-delay
		_ = upstream.Close()
		_ = downstream.Close()
		<-copied
	}()
	t.Cleanup(func() {
		resume()
		_ = listener.Close()
		select {
		case <-joined:
		case <-time.After(3 * time.Second):
			t.Error("registration fault proxy did not join")
		}
	})
	return listener.Addr().String(), resume
}

// scopeLockTracer groups each connection's statements into transactions and
// records, for every finished transaction that verified a Runtime scope, how
// many binding and process locks it took, in completion order.
type scopeLockTracer struct {
	mu   sync.Mutex
	open map[*pgx.Conn]*scopeLocks
	done []scopeLocks
}

type scopeLocks struct{ process, binding int }

func (tr *scopeLockTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	statement := strings.ToLower(strings.TrimSpace(data.SQL))
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.open == nil {
		tr.open = make(map[*pgx.Conn]*scopeLocks)
	}
	switch {
	case strings.HasPrefix(statement, "begin"):
		tr.open[conn] = &scopeLocks{}
	case statement == "commit" || statement == "rollback":
		if locks := tr.open[conn]; locks != nil {
			if locks.process+locks.binding > 0 {
				tr.done = append(tr.done, *locks)
			}
			delete(tr.open, conn)
		}
	default:
		if locks := tr.open[conn]; locks != nil {
			if strings.Contains(data.SQL, servingProcessLockSQL) {
				locks.process++
			}
			if strings.Contains(data.SQL, servingBindingLockSQL) {
				locks.binding++
			}
		}
	}
	return ctx
}

func (*scopeLockTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (tr *scopeLockTracer) take() []scopeLocks {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	done := tr.done
	tr.done = nil
	return done
}

// Each non-Sandbox scope-verified transaction locks its binding and process
// once: the receipt-scope proof's process SHARE lock also authorizes new
// work. FinishIdle's final adoption transaction follows the unchanged
// Sandbox-serving output-capture transaction, which still locks twice. After a
// same-Pod promotion the unchanged binding still replays exact receipts under
// one lock and rejects new work.
func TestPostgreSQLRuntimeScopeTransactionsLockBindingAndProcessOnce(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_single_process_lock"
		threadID  = "thr_single_process_lock"
		bindingID = "bind_single_process_lock"
		podUID    = "pod_single_process_lock"
		request   = "mreq_single_process_lock"
	)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	tracer := &scopeLockTracer{}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(storagetest.OpenRuntimeRoleDBWithTracer(t, runtime, tracer)))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	scope := sessionfixture.BridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	ctx := context.Background()
	once := []scopeLocks{{process: 1, binding: 1}}
	verified := func(name string, want []scopeLocks, call func() error) {
		t.Helper()
		tracer.take()
		if err := call(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := tracer.take(); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s took process/binding locks per transaction %+v; want %+v", name, got, want)
		}
	}
	outcome := func(ok bool, response any) error {
		if !ok {
			return status.Errorf(codes.Internal, "unexpected outcome %v", response)
		}
		return nil
	}
	declare := func(name, writeID string, declaration *bridgev1.RuntimeToolDeclaration) string {
		t.Helper()
		var eventID string
		verified(name, once, func() error {
			response, err := store.WriteEvent(ctx, &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: writeID, ModelRequestId: request, ToolDeclaration: declaration})
			if err != nil {
				return err
			}
			eventID = response.GetCommitted().GetEventId()
			return outcome(eventID != "", response)
		})
		return eventID
	}

	verified("request start", once, func() error {
		response, err := store.WriteEvent(ctx, &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: "single-lock-start", ModelRequestId: request, EventType: "span.model_request_start", PayloadJson: `{"type":"span.model_request_start","model_request_id":"` + request + `"}`, ContextThroughMessageSequence: sessionfixture.BridgeAPIInt64(0), RequestKind: "agent_provider_request"})
		if err != nil {
			return err
		}
		return outcome(response.GetCommitted() != nil, response)
	})
	mcpToolUse := declare("MCP Tool declaration", "single-lock-mcp", bridgeMCPToolDeclarationForTest("call_single_lock_mcp", "create_issue", "github", `{"title":"Bug","body":"Details"}`, "allow"))
	spawnToolUse := declare("spawn Tool declaration", "single-lock-spawn", sessionfixture.BridgeToolDeclarationWithRouteForTest("call_single_lock_spawn", "spawn_agent", `{"task_name":"single-lock","agent_type":"general","fork_turns":"none","prompt":"work"}`, "allow"))
	claim := &bridgev1.ClaimMcpToolResultRequest{Scope: scope, ToolUseEventId: mcpToolUse, ClaimId: "claim_single_lock"}
	verified("MCP claim", once, func() error {
		response, err := store.ClaimMcpToolResult(ctx, claim)
		if err != nil {
			return err
		}
		return outcome(response.GetAcquired() != nil, response)
	})
	verified("MCP claim renewal", once, func() error {
		response, err := store.ClaimMcpToolResult(ctx, claim)
		if err != nil {
			return err
		}
		return outcome(response.GetAcquired() != nil, response)
	})
	verified("MCP relinquish", once, func() error {
		response, err := store.RelinquishMcpToolResult(ctx, &bridgev1.RelinquishMcpToolResultRequest{Scope: scope, ToolUseEventId: mcpToolUse, ClaimId: claim.GetClaimId()})
		if err != nil {
			return err
		}
		return outcome(response.GetRelinquished() != nil, response)
	})
	claim.ClaimId = "claim_single_lock_again"
	verified("MCP claim after relinquish", once, func() error {
		response, err := store.ClaimMcpToolResult(ctx, claim)
		if err != nil {
			return err
		}
		return outcome(response.GetAcquired() != nil, response)
	})
	commit := &bridgev1.CommitMcpToolResultRequest{Scope: scope, ToolUseEventId: mcpToolUse, ClaimId: claim.GetClaimId(), ResultJson: `{"response":{"status":1,"result_text":"created","attachments":[]},"content_items":1,"refresh_triggered":false}`}
	verified("MCP commit preflight and commit", []scopeLocks{{1, 1}, {1, 1}}, func() error {
		response, err := store.CommitMcpToolResult(ctx, commit)
		if err != nil {
			return err
		}
		return outcome(response.GetCommitted() != nil, response)
	})
	create := &bridgev1.CreateSubagentThreadRequest{Scope: scope, SourceToolUseEventId: spawnToolUse, TaskName: "single-lock-worker", AgentType: "worker", InitialPrompt: "work"}
	var childID string
	verified("child creation", once, func() error {
		response, err := store.CreateSubagentThread(ctx, create)
		if err != nil {
			return err
		}
		childID = response.GetCommitted().GetChildThreadId()
		return outcome(childID != "", response)
	})
	mailToolUse := declare("send_message Tool declaration", "single-lock-mail", sessionfixture.BridgeToolDeclarationWithRouteForTest("call_single_lock_mail", "send_message", `{"task_name":"single-lock-worker","message":"hello"}`, "allow"))
	verified("inter-agent mail", once, func() error {
		response, err := store.DeliverInterAgentMail(ctx, &bridgev1.DeliverInterAgentMailRequest{Scope: scope, DeliveryId: runtimecontrol.AgentMailDeliveryID(mailToolUse, childID), TargetThreadId: childID, SourceToolUseEventId: mailToolUse, Content: "hello"})
		if err != nil {
			return err
		}
		return outcome(response.GetCommitted() != nil, response)
	})
	resumeToolUse := declare("resume_agent Tool declaration", "single-lock-resume", sessionfixture.BridgeToolDeclarationWithRouteForTest("call_single_lock_resume", "resume_agent", `{"task_name":"single-lock-worker"}`, "allow"))
	verified("child resume", once, func() error {
		response, err := store.MarkChildThreadActive(ctx, &bridgev1.MarkChildThreadActiveRequest{Scope: scope, SourceToolUseEventId: resumeToolUse, TargetChildThreadId: childID})
		if err != nil {
			return err
		}
		return outcome(response.GetCommitted() != nil, response)
	})
	verified("internal Tool repair", once, func() error {
		response, err := store.CommitInternalToolRepair(ctx, &bridgev1.CommitInternalToolRepairRequest{
			Scope: scope, ModelRequestId: request, ModelToolCallId: "call_single_lock_repair", ToolName: "unknown_tool",
			RepairKey: internalToolRepairKey(request, "call_single_lock_repair", "unknown_tool"), CanonicalInputJson: `{"q":"x"}`,
			Error: &bridgev1.RuntimeToolError{ErrorJson: `{"type":"provider_tool_protocol_error","message":"invalid tool","retryable":false}`},
		})
		if err != nil {
			return err
		}
		return outcome(response.GetCommitted() != nil, response)
	})
	verified("approval reviewer trunk", once, func() error {
		response, err := store.EnsureApprovalReviewerTrunk(ctx, &bridgev1.EnsureApprovalReviewerTrunkRequest{Scope: scope, EnsureOperationId: "ensure_single_lock_reviewer"})
		if err != nil {
			return err
		}
		return outcome(response.GetCommitted().GetReviewerThreadId() != "", response)
	})
	verified("approval reviewer sidecar", once, func() error {
		response, err := store.EnsureApprovalReviewerSidecar(ctx, &bridgev1.EnsureApprovalReviewerSidecarRequest{Scope: scope, ReviewId: "review_single_lock"})
		if err != nil {
			return err
		}
		return outcome(response.GetCommitted().GetReviewerThreadId() != "", response)
	})
	verified("current-scope read", once, func() error {
		response, err := store.ListChildThreads(ctx, &bridgev1.ListChildThreadsRequest{Scope: scope})
		if err != nil {
			return err
		}
		return outcome(len(response.GetCompleted().GetChildren()) > 0, response)
	})

	// FinishIdle in its own Session: the Sandbox-serving output-capture
	// admission keeps its two locks; the final adoption transaction takes one.
	const idleSessionID, idleThreadID, idleBindingID, idlePodUID, idleTurn = "sesn_single_lock_idle", "thr_single_lock_idle", "bind_single_lock_idle", "pod_single_lock_idle", "evt_single_lock_idle_turn"
	sessionfixture.SeedBridgeAPISession(t, admin, "default", idleSessionID, idleThreadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", idleSessionID, idleBindingID, 1, idlePodUID)
	idleScope := sessionfixture.BridgeAPIScope(idleSessionID, idleThreadID, idleBindingID, 1, idlePodUID)
	seedBridgeAPIOpenDurableTurn(t, admin, idleScope, idleTurn)
	verified("FinishIdle", []scopeLocks{{2, 2}, {1, 1}}, func() error {
		response, err := finishIdleWithStagedCaptureForTest(t, admin, store, &bridgev1.FinishIdleRequest{Scope: idleScope, DurableTurnId: idleTurn, StopReasonJson: `{"type":"end_turn"}`})
		if err != nil {
			return err
		}
		return outcome(response.GetCommitted() != nil, response)
	})

	identity := runtimecontrol.ProcessIdentity{Namespace: "tetral-agent-runtime", PodUID: podUID, ID: "process_single_lock_replacement"}
	replacement, err := runtimecontrol.RegisterProcess(ctx, dbconnect.NewClientForTesting(admin), identity)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := runtimecontrol.ReportProcess(ctx, dbconnect.NewClientForTesting(admin), identity, replacement.RegistrationReceipt, runtimecontrol.ProcessAccepting); err != nil {
		t.Fatal(err)
	}
	verified("retired child creation replay", once, func() error {
		response, err := store.CreateSubagentThread(ctx, create)
		if err != nil {
			return err
		}
		return outcome(response.GetDuplicate().GetChildThreadId() == childID, response)
	})
	verified("retired MCP commit replay", once, func() error {
		response, err := store.CommitMcpToolResult(ctx, commit)
		if err != nil {
			return err
		}
		return outcome(response.GetDuplicate() != nil, response)
	})
	verified("retired new event", once, func() error {
		response, err := store.WriteEvent(ctx, &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: "single-lock-after-promotion", EventType: "session.status_running", PayloadJson: `{"type":"session.status_running"}`})
		if err != nil {
			if status.Code(err) == codes.FailedPrecondition {
				return nil
			}
			return err
		}
		return outcome(response.GetStale() != nil, response)
	})
}
