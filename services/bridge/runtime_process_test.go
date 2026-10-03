package agentruntimebridge

import (
	"context"
	"database/sql"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func processRegistryRPC(t *testing.T, client *dbconnect.Client, podUID string) bridgev1.AgentRuntimeBridgeServiceClient {
	return processRegistryRPCWithStore(t, NewPostgreSQLBridgeAPIStore(client), podUID, nil)
}

func processRegistryRPCWithStore(t *testing.T, store *PostgreSQLBridgeAPIStore, podUID string, after func(context.Context, string, any) error) bridgev1.AgentRuntimeBridgeServiceClient {
	t.Helper()
	identity := auth.Identity{ServiceAccount: auth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"}, KubernetesPodUID: podUID}
	return processRegistryRPCWithIdentity(t, store, identity, after)
}
func processRegistryRPCWithIdentity(t *testing.T, store *PostgreSQLBridgeAPIStore, identity auth.Identity, after func(context.Context, string, any) error) bridgev1.AgentRuntimeBridgeServiceClient {
	client, _ := processRegistryRPCWithIdentityAddress(t, store, identity, after)
	return client
}
func processRegistryRPCWithIdentityAddress(t *testing.T, store *PostgreSQLBridgeAPIStore, identity auth.Identity, after func(context.Context, string, any) error) (bridgev1.AgentRuntimeBridgeServiceClient, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := BridgeAPIMethodAuthorizer(identity, info.FullMethod); err != nil {
			return nil, err
		}
		reply, err := handler(auth.ContextWithIdentity(ctx, identity), request)
		if err == nil && after != nil {
			err = after(ctx, info.FullMethod, reply)
		}
		return reply, err
	}))
	RegisterBridgeAPI(server, store)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("process registry RPC did not join")
		}
	})
	return bridgev1.NewAgentRuntimeBridgeServiceClient(conn), listener.Addr().String()
}

func TestPostgreSQLRuntimeProcessLiveness(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	storeClient := dbconnect.NewClientForTesting(runtime)
	a := processRegistryRPC(t, storeClient, "pod-registry")
	b := processRegistryRPC(t, storeClient, "pod-registry")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	register := func(client bridgev1.AgentRuntimeBridgeServiceClient, id string) *bridgev1.RegisterRuntimeProcessResponse {
		t.Helper()
		response, err := client.RegisterRuntimeProcess(ctx, &bridgev1.RegisterRuntimeProcessRequest{RuntimeProcessId: id})
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	report := func(client bridgev1.AgentRuntimeBridgeServiceClient, r *bridgev1.RegisterRuntimeProcessResponse, phase bridgev1.RuntimeProcessPhase) error {
		_, err := client.ReportRuntimeProcess(ctx, &bridgev1.ReportRuntimeProcessRequest{RuntimeProcessId: r.RuntimeProcessId, RegistrationReceipt: r.RegistrationReceipt, Phase: phase})
		return err
	}
	accepting := bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_ACCEPTING
	draining := bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_DRAINING
	p1 := register(a, "process-one")
	p1Replay := register(b, "process-one")
	if p1.RegistrationOrder != p1Replay.RegistrationOrder || p1.RegistrationReceipt != p1Replay.RegistrationReceipt {
		t.Fatal("registration retry allocated new custody")
	}
	if err := report(a, p1, accepting); err != nil {
		t.Fatal(err)
	}
	// A late registration from a dead boot is only a candidate. Comparing the
	// promoted order, rather than the highest allocation, allows the live boot.
	p2 := register(a, "process-two")
	dead := register(b, "dead-delayed-register")
	if err := report(b, p2, accepting); err != nil {
		t.Fatal(err)
	}
	var current string
	if err := admin.QueryRow(`SELECT runtime_process_id FROM runtime_processes WHERE is_current AND pod_uid='pod-registry'`).Scan(&current); err != nil || current != p2.RuntimeProcessId {
		t.Fatalf("current=%s err=%v", current, err)
	}
	if err := report(b, p1, accepting); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("retired promotion: %v", err)
	}
	if _, err := b.RegisterRuntimeProcess(ctx, &bridgev1.RegisterRuntimeProcessRequest{RuntimeProcessId: p1.RuntimeProcessId}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("retired registration: %v", err)
	}
	if err := report(b, p2, accepting); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ReportRuntimeProcess(ctx, &bridgev1.ReportRuntimeProcessRequest{RuntimeProcessId: dead.RuntimeProcessId, RegistrationReceipt: "guessed", Phase: accepting}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("guessed receipt: %v", err)
	}
	abandoned := register(a, "abandoned")
	if err := report(a, abandoned, draining); err != nil {
		t.Fatal(err)
	}
	if err := report(b, abandoned, accepting); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("abandoned promotion: %v", err)
	}
	if err := report(a, p2, draining); err != nil {
		t.Fatal(err)
	}
	if err := report(b, p2, accepting); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("phase reversal: %v", err)
	}
	p3 := register(b, "process-three")
	if err := report(b, p3, accepting); err != nil {
		t.Fatal(err)
	}
	if err := report(a, dead, accepting); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("lower candidate promoted: %v", err)
	}
	if err := admin.QueryRow(`SELECT runtime_process_id FROM runtime_processes WHERE is_current AND pod_uid='pod-registry'`).Scan(&current); err != nil || current != p3.RuntimeProcessId {
		t.Fatalf("current=%s err=%v", current, err)
	}

	t.Run("promotion waits for held process authority", func(t *testing.T) {
		next := register(a, "process-next")
		locked, release := make(chan struct{}), make(chan struct{})
		finished := make(chan error, 1)
		go func() {
			finished <- dbconnect.NewClientForTesting(admin).WithTx(ctx, "runtimecontrol.test_process_fence", nil, func(tx *dbconnect.Tx) error {
				if _, err := runtimecontrol.RequireCurrentProcessTx(ctx, tx, runtimecontrol.ProcessIdentity{Namespace: "tetral-agent-runtime", PodUID: "pod-registry", ID: p3.RuntimeProcessId}); err != nil {
					return err
				}
				close(locked)
				<-release
				return nil
			})
		}()
		select {
		case <-locked:
		case <-ctx.Done():
			t.Fatal("process lock not reached")
		}
		promoted := make(chan error, 1)
		go func() { promoted <- report(b, next, accepting) }()
		// Observe the actual PostgreSQL row-lock wait before releasing the writer.
		waitCtx, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		for {
			var waiting bool
			err := admin.QueryRowContext(waitCtx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%runtime_processes%')`).Scan(&waiting)
			if err != nil {
				close(release)
				t.Fatal(err)
			}
			if waiting {
				break
			}
			select {
			case err := <-promoted:
				close(release)
				t.Fatalf("promotion bypassed process share lock: %v", err)
			case <-waitCtx.Done():
				close(release)
				t.Fatal("database lock barrier not reached")
			case <-time.After(time.Millisecond):
			}
		}
		close(release)
		if err := <-finished; err != nil {
			t.Fatal(err)
		}
		if err := <-promoted; err != nil {
			t.Fatal(err)
		}
		err := dbconnect.NewClientForTesting(admin).WithTx(ctx, "runtimecontrol.test_process_fenced", nil, func(tx *dbconnect.Tx) error {
			_, err := runtimecontrol.RequireCurrentProcessTx(ctx, tx, runtimecontrol.ProcessIdentity{Namespace: "tetral-agent-runtime", PodUID: "pod-registry", ID: p3.RuntimeProcessId})
			return err
		})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("promoted winner did not fence old process: %v", err)
		}
	})
}

func TestPostgreSQLRuntimeProcessRegistrationAndReportResponseLoss(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	client := dbconnect.NewClientForTesting(runtime)
	committed := make(chan string, 2)
	lost := processRegistryRPCWithStore(t, NewPostgreSQLBridgeAPIStore(client), "pod-lost-ack", func(ctx context.Context, method string, reply any) error {
		committed <- method
		<-ctx.Done()
		return status.Error(codes.Canceled, "held response was canceled")
	})
	replay := processRegistryRPC(t, client, "pod-lost-ack")
	ctx, cancelAll := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelAll()
	requestCtx, cancel := context.WithCancel(ctx)
	response := make(chan error, 1)
	go func() {
		_, err := lost.RegisterRuntimeProcess(requestCtx, &bridgev1.RegisterRuntimeProcessRequest{RuntimeProcessId: "lost-ack-process"})
		response <- err
	}()
	select {
	case <-committed:
	case <-ctx.Done():
		t.Fatal("registration response barrier was not reached")
	}
	var order int64
	var receipt string
	if err := admin.QueryRowContext(ctx, `SELECT registration_order,registration_receipt FROM runtime_processes WHERE pod_uid='pod-lost-ack' AND runtime_process_id='lost-ack-process'`).Scan(&order, &receipt); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-response; status.Code(err) != codes.Canceled {
		t.Fatalf("lost register response: %v", err)
	}
	registered, err := replay.RegisterRuntimeProcess(ctx, &bridgev1.RegisterRuntimeProcessRequest{RuntimeProcessId: "lost-ack-process"})
	if err != nil {
		t.Fatal(err)
	}
	if registered.RegistrationOrder != order || registered.RegistrationReceipt != receipt {
		t.Fatal("lost registration response retry allocated new identity")
	}
	requestCtx, cancel = context.WithCancel(ctx)
	report := &bridgev1.ReportRuntimeProcessRequest{RuntimeProcessId: registered.RuntimeProcessId, RegistrationReceipt: registered.RegistrationReceipt, Phase: bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_ACCEPTING}
	go func() { _, err := lost.ReportRuntimeProcess(requestCtx, report); response <- err }()
	select {
	case <-committed:
	case <-ctx.Done():
		t.Fatal("report response barrier was not reached")
	}
	var current bool
	var reportedAt time.Time
	if err := admin.QueryRowContext(ctx, `SELECT is_current,reported_at FROM runtime_processes WHERE pod_uid='pod-lost-ack' AND runtime_process_id='lost-ack-process'`).Scan(&current, &reportedAt); err != nil {
		t.Fatal(err)
	}
	if !current || reportedAt.IsZero() {
		t.Fatal("report barrier preceded durable promotion")
	}
	cancel()
	if err := <-response; status.Code(err) != codes.Canceled {
		t.Fatalf("lost report response: %v", err)
	}
	ack, err := replay.ReportRuntimeProcess(ctx, report)
	if err != nil {
		t.Fatal(err)
	}
	if !ack.Current || ack.Phase != report.Phase {
		t.Fatal("exact report replay did not acknowledge current accepting phase")
	}
	var promotedOrder int64
	if err := admin.QueryRowContext(ctx, `SELECT last_promoted_order FROM runtime_process_pods WHERE pod_uid='pod-lost-ack'`).Scan(&promotedOrder); err != nil {
		t.Fatal(err)
	}
	if promotedOrder != order {
		t.Fatal("report replay changed promotion order")
	}
}

func TestPostgreSQLRuntimeProcessReportUsesConfiguredAttemptBudget(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.ProcessPolicy.ReportTimeout = 2 * time.Second
	store.ProcessPolicy.ReportInterval = 3 * time.Second
	client := processRegistryRPCWithStore(t, store, "pod-custom-budget", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	registered, err := client.RegisterRuntimeProcess(ctx, &bridgev1.RegisterRuntimeProcessRequest{RuntimeProcessId: "custom-budget-process"})
	if err != nil {
		t.Fatal(err)
	}
	blocker, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback() }()
	if _, err := blocker.ExecContext(ctx, `SELECT next_registration_order FROM runtime_process_pods WHERE pod_uid='pod-custom-budget' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := client.ReportRuntimeProcess(ctx, &bridgev1.ReportRuntimeProcessRequest{RuntimeProcessId: registered.RuntimeProcessId, RegistrationReceipt: registered.RegistrationReceipt, Phase: bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_ACCEPTING})
		result <- err
	}()
	for {
		var waiting bool
		if err := admin.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%last_promoted_order%')`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-result:
			t.Fatalf("configured report missed lock barrier: %v", err)
		case <-ctx.Done():
			t.Fatal("configured report lock barrier was not reached")
		case <-time.After(time.Millisecond):
		}
	}
	select {
	case err := <-result:
		t.Fatalf("configured report remained clipped to default one-second budget: %v", err)
	case <-time.After(1100 * time.Millisecond):
	}
	if err := blocker.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("configured report did not finish after row lock release")
	}
}

func TestPostgreSQLRuntimeProcessReportDeadlineFencesPromotion(t *testing.T) {
	for _, variant := range []string{"configured server deadline", "earlier caller deadline", "parent cancellation"} {
		t.Run(variant, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			client := dbconnect.NewClientForTesting(runtime)
			store := NewPostgreSQLBridgeAPIStore(client)
			store.ProcessPolicy.ReportTimeout = 80 * time.Millisecond
			rpc, returned := receiptJoinedRPC(t, store, "pod-report-deadline")
			ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			registered, err := rpc.RegisterRuntimeProcess(ctx, &bridgev1.RegisterRuntimeProcessRequest{RuntimeProcessId: "report-deadline"})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-returned:
			case <-ctx.Done():
				t.Fatal("registration handler did not join")
			}
			beforePromotion := reportDeadlinePromotionSnapshot(t, admin, "pod-report-deadline")
			var before string
			if err := admin.QueryRow(`SELECT to_jsonb(process)::text FROM runtime_processes process WHERE runtime_process_id=$1`, registered.RuntimeProcessId).Scan(&before); err != nil {
				t.Fatal(err)
			}
			blocker, err := admin.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = blocker.Rollback() }()
			if _, err := blocker.ExecContext(ctx, `SELECT next_registration_order FROM runtime_process_pods WHERE pod_uid='pod-report-deadline' FOR UPDATE`); err != nil {
				t.Fatal(err)
			}
			caller := ctx
			cancel := func() {}
			switch variant {
			case "earlier caller deadline":
				caller, cancel = context.WithTimeout(ctx, 20*time.Millisecond)
			case "parent cancellation":
				caller, cancel = context.WithCancel(ctx)
			}
			defer cancel()
			done := make(chan error, 1)
			start := time.Now()
			report := &bridgev1.ReportRuntimeProcessRequest{RuntimeProcessId: registered.RuntimeProcessId, RegistrationReceipt: registered.RegistrationReceipt, Phase: bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_ACCEPTING}
			go func() { _, err := rpc.ReportRuntimeProcess(caller, report); done <- err }()
			// Parent cancellation is released only after independent PostgreSQL proves
			// the real report is waiting at its Pod arbitration row.
			if variant == "parent cancellation" {
				for {
					var waiting bool
					if err := admin.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%last_promoted_order%')`).Scan(&waiting); err != nil {
						t.Fatal(err)
					}
					if waiting {
						break
					}
					select {
					case err := <-done:
						t.Fatalf("report missed lock=%v", err)
					case <-ctx.Done():
						t.Fatal("report did not reach lock")
					case <-time.After(time.Millisecond):
					}
				}
				cancel()
			}
			want := codes.DeadlineExceeded
			if variant == "parent cancellation" {
				want = codes.Canceled
			}
			select {
			case err := <-done:
				if status.Code(err) != want {
					t.Fatalf("actual report phase status=%v want=%v", err, want)
				}
			case <-ctx.Done():
				t.Fatal("bounded report did not finish")
			}
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Fatalf("configured report bound=%s", elapsed)
			}
			select {
			case <-returned:
			case <-ctx.Done():
				t.Fatal("expired report handler did not join")
			}
			var after string
			if err := admin.QueryRow(`SELECT to_jsonb(process)::text FROM runtime_processes process WHERE runtime_process_id=$1`, registered.RuntimeProcessId).Scan(&after); err != nil {
				t.Fatal(err)
			}
			if before != after {
				t.Fatal("expired/cancelled report changed candidate row")
			}
			if err := blocker.Commit(); err != nil {
				t.Fatal(err)
			}
			lockProbe, err := admin.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = lockProbe.Rollback() }()
			var order int64
			if err := lockProbe.QueryRowContext(ctx, `SELECT last_promoted_order FROM runtime_process_pods WHERE namespace='tetral-agent-runtime' AND pod_uid='pod-report-deadline' FOR UPDATE NOWAIT`).Scan(&order); err != nil {
				t.Fatalf("independent Pod NOWAIT lock: %v", err)
			}
			var id string
			if err := lockProbe.QueryRowContext(ctx, `SELECT runtime_process_id FROM runtime_processes WHERE namespace='tetral-agent-runtime' AND pod_uid='pod-report-deadline' AND runtime_process_id=$1 FOR UPDATE NOWAIT`, registered.RuntimeProcessId).Scan(&id); err != nil {
				t.Fatalf("independent candidate NOWAIT lock: %v", err)
			}
			if err := lockProbe.Commit(); err != nil {
				t.Fatal(err)
			}
			if order != 0 || id != registered.RuntimeProcessId || reportDeadlinePromotionSnapshot(t, admin, "pod-report-deadline") != beforePromotion {
				t.Fatal("cancelled report changed full promotion state")
			}
			if err := client.Ping(ctx); err != nil {
				t.Fatalf("same actual runtime pool not ready: %v", err)
			}
			t.Logf("independent cleanup: initial=%s handler joined; Pod+candidate NOWAIT reusable; promotion state unchanged; same pool ready", variant)
			// The independent NOWAIT locks prove cancellation cleanup. A separate
			// normal-policy receiver proves exact-receipt liveness without imposing
			// the fault receiver's 80ms budget on a healthy promotion transaction.
			normalStore := NewPostgreSQLBridgeAPIStore(client)
			normalRPC := processRegistryRPCWithStore(t, normalStore, "pod-report-deadline", nil)
			response, err := normalRPC.ReportRuntimeProcess(ctx, report)
			if err != nil {
				t.Fatalf("normal-policy exact report after cancelled handler and independent lock reuse: %v", err)
			}
			if response.GetRuntimeProcessId() != registered.RuntimeProcessId || !response.GetCurrent() || response.GetPhase() != report.Phase {
				t.Fatalf("exact followup response=%v", response)
			}
			var current bool
			var phase, receipt string
			var promoted, registeredOrder int64
			if err := admin.QueryRow(`SELECT process.is_current,process.phase,process.registration_receipt,process.registration_order,pod.last_promoted_order FROM runtime_processes process JOIN runtime_process_pods pod USING(namespace,pod_uid) WHERE process.runtime_process_id=$1`, registered.RuntimeProcessId).Scan(&current, &phase, &receipt, &registeredOrder, &promoted); err != nil {
				t.Fatal(err)
			}
			if !current || phase != "accepting" || receipt != registered.RegistrationReceipt || registeredOrder != registered.RegistrationOrder || promoted != registeredOrder {
				t.Fatal("followup ACK disagrees with exact committed promotion tuple")
			}
			t.Log("normal-policy exact receipt ACK and committed promotion tuple verified")
		})
	}
}

func reportDeadlinePromotionSnapshot(t *testing.T, admin *sql.DB, podUID string) string {
	t.Helper()
	var state string
	if err := admin.QueryRow(`SELECT jsonb_build_object('processes',(SELECT jsonb_agg(to_jsonb(process) ORDER BY process.registration_order) FROM runtime_processes process WHERE namespace='tetral-agent-runtime' AND pod_uid=$1),'pod',(SELECT to_jsonb(pod) FROM runtime_process_pods pod WHERE namespace='tetral-agent-runtime' AND pod_uid=$1))::text`, podUID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	return state
}
