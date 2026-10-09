package agentruntimebridge

import (
	"context"
	"database/sql"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
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

	t.Run("identity, process ID and report shape rejections write no registry rows", func(t *testing.T) {
		const podUID = "pod-registry-rejections"
		store := NewPostgreSQLBridgeAPIStore(storeClient)
		runtimeIdentity := auth.Identity{ServiceAccount: auth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"}, KubernetesPodUID: podUID}
		valid := processRegistryRPCWithIdentity(t, store, runtimeIdentity, nil)
		wrongAccount := processRegistryRPCWithIdentity(t, store, auth.Identity{ServiceAccount: auth.ServiceAccount{Namespace: "tetral-system", Name: "mcp-connector"}, KubernetesPodUID: podUID}, nil)
		missingPod := processRegistryRPCWithIdentity(t, store, auth.Identity{ServiceAccount: runtimeIdentity.ServiceAccount}, nil)
		for name, client := range map[string]bridgev1.AgentRuntimeBridgeServiceClient{"wrong service account": wrongAccount, "missing Pod UID": missingPod} {
			if _, err := client.RegisterRuntimeProcess(ctx, &bridgev1.RegisterRuntimeProcessRequest{RuntimeProcessId: "process-rejected"}); status.Code(err) != codes.PermissionDenied {
				t.Fatalf("%s register: %v", name, err)
			}
			if _, err := client.ReportRuntimeProcess(ctx, &bridgev1.ReportRuntimeProcessRequest{RuntimeProcessId: "process-rejected", RegistrationReceipt: "receipt", Phase: accepting}); status.Code(err) != codes.PermissionDenied {
				t.Fatalf("%s report: %v", name, err)
			}
		}
		for name, id := range map[string]string{"empty": "", "129 bytes": strings.Repeat("p", 129), "leading space": " process", "NUL": "process\x00id", "newline": "process\nid"} {
			if _, err := valid.RegisterRuntimeProcess(ctx, &bridgev1.RegisterRuntimeProcessRequest{RuntimeProcessId: id}); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("%s process ID register: %v", name, err)
			}
			if _, err := valid.ReportRuntimeProcess(ctx, &bridgev1.ReportRuntimeProcessRequest{RuntimeProcessId: id, RegistrationReceipt: "receipt", Phase: accepting}); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("%s process ID report: %v", name, err)
			}
		}
		if _, err := valid.ReportRuntimeProcess(ctx, &bridgev1.ReportRuntimeProcessRequest{RuntimeProcessId: "process-rejected", RegistrationReceipt: "receipt", Phase: bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_UNSPECIFIED}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("unspecified phase: %v", err)
		}
		if _, err := valid.ReportRuntimeProcess(ctx, &bridgev1.ReportRuntimeProcessRequest{RuntimeProcessId: "process-rejected", Phase: accepting}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("empty receipt: %v", err)
		}
		var pods, processes int
		if err := admin.QueryRow(`SELECT count(*) FROM runtime_process_pods WHERE pod_uid IN ($1, '')`, podUID).Scan(&pods); err != nil {
			t.Fatal(err)
		}
		if err := admin.QueryRow(`SELECT count(*) FROM runtime_processes WHERE pod_uid IN ($1, '')`, podUID).Scan(&processes); err != nil {
			t.Fatal(err)
		}
		if pods != 0 || processes != 0 {
			t.Fatalf("rejected registry calls wrote pods=%d processes=%d", pods, processes)
		}
	})

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
	var reportedAt sql.NullTime
	if err := admin.QueryRowContext(ctx, `SELECT process.is_current,live.reported_at FROM runtime_processes process JOIN runtime_process_liveness live USING(namespace,pod_uid,runtime_process_id) WHERE process.pod_uid='pod-lost-ack' AND process.runtime_process_id='lost-ack-process'`).Scan(&current, &reportedAt); err != nil {
		t.Fatal(err)
	}
	if !current || !reportedAt.Valid {
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

// A handler can return while database/sql's cancellation rollback or pgx's
// native connection cleanup still runs. Trace BEGIN as well as the Pod query:
// expiry between those statements must not be mistaken for pre-SQL expiry.
type reportBackendCapture struct {
	pid          uint32
	backendStart string
	begin, pod   bool
	cleanup      <-chan struct{}
}
type reportBackendTracer struct {
	ctx        context.Context
	admin      *sql.DB
	mu         sync.Mutex
	armed      bool
	identities map[uint32]string
	backends   map[uint32]reportBackendCapture
	err        error
}

func (tracer *reportBackendTracer) TraceConnectStart(ctx context.Context, _ pgx.TraceConnectStartData) context.Context {
	return ctx
}
func (tracer *reportBackendTracer) TraceConnectEnd(_ context.Context, data pgx.TraceConnectEndData) {
	if data.Err != nil {
		return
	}
	pid := data.Conn.PgConn().PID()
	var identity string
	err := tracer.admin.QueryRowContext(tracer.ctx, `SELECT backend_start::text FROM pg_stat_activity WHERE datname=current_database() AND pid=$1`, pid).Scan(&identity)
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	if err != nil {
		tracer.err = err
		return
	}
	if tracer.identities == nil {
		tracer.identities = make(map[uint32]string)
	}
	tracer.identities[pid] = identity
}
func (tracer *reportBackendTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	begin := strings.EqualFold(strings.TrimSpace(data.SQL), "begin")
	pod := strings.HasPrefix(data.SQL, "SELECT last_promoted_order FROM runtime_process_pods")
	if !begin && !pod {
		return ctx
	}
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	if !tracer.armed {
		return ctx
	}
	pid := conn.PgConn().PID()
	capture := tracer.backends[pid]
	capture.pid = pid
	capture.backendStart = tracer.identities[pid]
	capture.begin = capture.begin || begin
	capture.pod = capture.pod || pod
	capture.cleanup = conn.PgConn().CleanupDone()
	tracer.backends[pid] = capture
	return ctx
}
func (*reportBackendTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}
func (tracer *reportBackendTracer) arm() {
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	tracer.armed = true
	tracer.backends = make(map[uint32]reportBackendCapture)
}
func (tracer *reportBackendTracer) snapshot() ([]reportBackendCapture, error) {
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	captures := make([]reportBackendCapture, 0, len(tracer.backends))
	for _, capture := range tracer.backends {
		captures = append(captures, capture)
	}
	return captures, tracer.err
}

type reportBackendState struct {
	backendStart string
	xactStart    sql.NullString
	state, wait  string
	locks        int
}

func readReportBackendState(ctx context.Context, admin *sql.DB, pid uint32) (reportBackendState, bool, error) {
	var state reportBackendState
	err := admin.QueryRowContext(ctx, `SELECT backend_start::text,xact_start::text,state,COALESCE(wait_event_type,''),(SELECT count(*) FROM pg_locks WHERE pid=$1 AND (locktype IN ('transactionid','virtualxid') OR relation='runtime_process_pods'::regclass)) FROM pg_stat_activity WHERE datname=current_database() AND pid=$1`, pid).Scan(&state.backendStart, &state.xactStart, &state.state, &state.wait, &state.locks)
	if err == sql.ErrNoRows {
		return state, false, nil
	}
	return state, true, err
}
func awaitReportBackendTerminal(ctx context.Context, t *testing.T, admin *sql.DB, capture reportBackendCapture) {
	t.Helper()
	if capture.backendStart == "" {
		t.Fatalf("report backend pid=%d has no independently bound identity", capture.pid)
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		state, exists, err := readReportBackendState(ctx, admin, capture.pid)
		if err != nil {
			t.Fatal(err)
		}
		var terminal string
		switch {
		case !exists:
			terminal = "backend_gone"
		case state.backendStart != capture.backendStart:
			terminal = "backend_replaced"
		case state.state == "idle" && !state.xactStart.Valid && state.locks == 0:
			terminal = "transaction_ended"
		}
		if terminal != "" {
			// Healthy pooled connections remain open. Only a departed original
			// backend requires native socket cleanup to join as well.
			if terminal != "transaction_ended" {
				select {
				case <-capture.cleanup:
				case <-ctx.Done():
					t.Fatal("departed report backend native cleanup did not join")
				}
			}
			t.Logf("report backend terminal=%s pid=%d backend=%s BEGIN=%t Pod=%t state=%s xact=%s locks=%d", terminal, capture.pid, capture.backendStart, capture.begin, capture.pod, state.state, state.xactStart.String, state.locks)
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("report backend not terminal: pid=%d state=%s xact=%s wait=%s locks=%d: %v", capture.pid, state.state, state.xactStart.String, state.wait, state.locks, ctx.Err())
		case <-ticker.C:
		}
	}
}
func TestPostgreSQLRuntimeProcessReportDeadlineFencesPromotion(t *testing.T) {
	for _, variant := range []string{"configured server deadline", "earlier caller deadline", "parent cancellation"} {
		t.Run(variant, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			tracer := &reportBackendTracer{ctx: ctx, admin: admin}
			client := dbconnect.NewClientForTesting(storagetest.OpenRuntimeRoleDBWithTracer(t, runtime, tracer))
			store := NewPostgreSQLBridgeAPIStore(client)
			store.ProcessPolicy.ReportTimeout = 80 * time.Millisecond
			rpc, returned := receiptJoinedRPC(t, store, "pod-report-deadline")
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
			tracer.arm()
			start := time.Now()
			report := &bridgev1.ReportRuntimeProcessRequest{RuntimeProcessId: registered.RuntimeProcessId, RegistrationReceipt: registered.RegistrationReceipt, Phase: bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_ACCEPTING}
			go func() { _, err := rpc.ReportRuntimeProcess(caller, report); done <- err }()
			// Parent cancellation is released only after independent PostgreSQL proves
			// the real report is waiting at its Pod arbitration row.
			if variant == "parent cancellation" {
				for {
					captures, err := tracer.snapshot()
					if err != nil {
						t.Fatal(err)
					}
					waiting := false
					for _, capture := range captures {
						if !capture.pod {
							continue
						}
						state, exists, err := readReportBackendState(ctx, admin, capture.pid)
						if err != nil {
							t.Fatal(err)
						}
						if exists && state.backendStart == capture.backendStart && state.state == "active" && state.wait == "Lock" && state.xactStart.Valid {
							waiting = true
							t.Logf("exact report Pod wait pid=%d backend=%s xact=%s", capture.pid, capture.backendStart, state.xactStart.String)
						}
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
			captures, err := tracer.snapshot()
			if err != nil {
				t.Fatal(err)
			}
			if len(captures) == 0 {
				if variant == "parent cancellation" {
					t.Fatal("parent cancellation lacked exact report SQL capture")
				}
				t.Log("report expired before any traced report SQL; no report backend transaction was started")
			}
			// Keep our blocker until the exact remote transaction ends. Go
			// handler return alone cannot join native asynchronous cleanup.
			// This is a backend ownership fence, not a retry of NOWAIT.
			for _, capture := range captures {
				awaitReportBackendTerminal(ctx, t, admin, capture)
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

// livenessTestPolicy keeps lifecycle reports that wait on barriers inside
// their attempt budget; the fences under test, not the timeout, decide.
func livenessTestPolicy(store *PostgreSQLBridgeAPIStore) *PostgreSQLBridgeAPIStore {
	store.ProcessPolicy.ReportTimeout = 5 * time.Second
	store.ProcessPolicy.ReportInterval = 6 * time.Second
	return store
}

type livenessFixture struct {
	t        *testing.T
	ctx      context.Context
	admin    *sql.DB
	registry bridgev1.AgentRuntimeBridgeServiceClient
	scope    *bridgev1.RuntimeScope
	receipt  string
}

func newLivenessFixture(ctx context.Context, t *testing.T, runtime, admin *sql.DB, suffix string) *livenessFixture {
	t.Helper()
	sessionID, threadID, bindingID, podUID := "sesn_liveness_"+suffix, "thr_liveness_"+suffix, "bind_liveness_"+suffix, "pod_liveness_"+suffix
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	f := &livenessFixture{t: t, ctx: ctx, admin: admin, scope: sessionfixture.BridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)}
	f.registry = processRegistryRPCWithStore(t, livenessTestPolicy(NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))), podUID, nil)
	if err := admin.QueryRow(`SELECT registration_receipt FROM runtime_processes WHERE runtime_process_id=$1`, f.scope.Binding.RuntimeProcessId).Scan(&f.receipt); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *livenessFixture) report(processID, receipt string, phase bridgev1.RuntimeProcessPhase) error {
	_, err := f.registry.ReportRuntimeProcess(f.ctx, &bridgev1.ReportRuntimeProcessRequest{RuntimeProcessId: processID, RegistrationReceipt: receipt, Phase: phase})
	return err
}

func (f *livenessFixture) register(processID string) *bridgev1.RegisterRuntimeProcessResponse {
	f.t.Helper()
	response, err := f.registry.RegisterRuntimeProcess(f.ctx, &bridgev1.RegisterRuntimeProcessRequest{RuntimeProcessId: processID})
	if err != nil {
		f.t.Fatal(err)
	}
	return response
}

func (f *livenessFixture) reportedAt(processID string) sql.NullTime {
	f.t.Helper()
	var reported sql.NullTime
	if err := f.admin.QueryRow(`SELECT reported_at FROM runtime_process_liveness WHERE pod_uid=$1 AND runtime_process_id=$2`, f.scope.Binding.TargetPodUid, processID).Scan(&reported); err != nil {
		f.t.Fatal(err)
	}
	return reported
}

// lifecycle returns the process's lifecycle facts and the Pod's current process.
func (f *livenessFixture) lifecycle(processID string) (phase string, current, retired bool, owner string) {
	f.t.Helper()
	if err := f.admin.QueryRow(`SELECT phase,is_current,retired_at IS NOT NULL,(SELECT string_agg(runtime_process_id,',') FROM runtime_processes WHERE pod_uid=$1 AND is_current) FROM runtime_processes WHERE pod_uid=$1 AND runtime_process_id=$2`, f.scope.Binding.TargetPodUid, processID).Scan(&phase, &current, &retired, &owner); err != nil {
		f.t.Fatal(err)
	}
	return phase, current, retired, owner
}

// awaitBlocked fails unless the lifecycle report is still waiting.
func awaitBlocked(t *testing.T, result <-chan error, what string) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("%s did not wait: %v", what, err)
	default:
	}
}

// An unchanged report updates only its liveness row, so it commits while a
// Session mutation holds the process row FOR SHARE. A phase change or promotion
// still waits for that mutation, which commits first; afterwards the retired
// process cannot start new work.
func TestPostgreSQLRuntimeUnchangedReportCommitsPastHeldMutationFence(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	f := newLivenessFixture(ctx, t, runtime, admin, "held_fence")
	trace := &bridgeExecutionQueryTracer{}
	writer := processRegistryRPCWithStore(t, newAwaitNotificationTracedStore(t, runtime, trace), f.scope.Binding.TargetPodUid, nil)
	write := func(id string) (*bridgev1.WriteEventResponse, error) {
		return writer.WriteEvent(ctx, &bridgev1.WriteEventRequest{Scope: f.scope, RuntimeWriteId: id, EventType: "session.status_running", PayloadJson: `{"type":"session.status_running"}`})
	}
	// heldWrite parks one mutation right after its process SHARE lock.
	heldWrite := func(id string) (func(), <-chan error) {
		t.Helper()
		fired, release := trace.armBarrier("", servingProcessLockSQL, 1)
		done := make(chan error, 1)
		go func() {
			response, err := write(id)
			if err == nil && response.GetCommitted() == nil {
				err = status.Errorf(codes.Internal, "held mutation %s outcome %v", id, response)
			}
			done <- err
		}()
		select {
		case <-fired:
		case <-ctx.Done():
			t.Fatal("mutation did not reach its process lock")
		}
		var once sync.Once
		return func() { once.Do(func() { close(release) }) }, done
	}
	accepting := bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_ACCEPTING
	draining := bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_DRAINING
	old := f.scope.Binding.RuntimeProcessId

	release, written := heldWrite("held-accepting")
	defer release()
	before := f.reportedAt(old)
	if err := f.report(old, f.receipt, accepting); err != nil {
		t.Fatalf("unchanged accepting report behind a held mutation: %v", err)
	}
	if after := f.reportedAt(old); !after.Valid || !after.Time.After(before.Time) {
		t.Fatalf("unchanged report did not commit its liveness: %v -> %v", before, after)
	}
	drained := make(chan error, 1)
	go func() { drained <- f.report(old, f.receipt, draining) }()
	waitProcessSQLLock(ctx, t, admin, "ORDER BY registration_order FOR UPDATE")
	awaitBlocked(t, drained, "phase change behind a held mutation")
	if phase, _, _, _ := f.lifecycle(old); phase != runtimecontrol.ProcessAccepting {
		t.Fatalf("phase changed to %s before the held mutation committed", phase)
	}
	release()
	if err := <-written; err != nil {
		t.Fatalf("held mutation: %v", err)
	}
	if err := <-drained; err != nil {
		t.Fatalf("phase change after the mutation: %v", err)
	}

	next := f.register("held-fence-next")
	release, written = heldWrite("held-draining")
	defer release()
	before = f.reportedAt(old)
	if err := f.report(old, f.receipt, draining); err != nil {
		t.Fatalf("unchanged draining report behind a held mutation: %v", err)
	}
	if after := f.reportedAt(old); !after.Valid || !after.Time.After(before.Time) {
		t.Fatalf("unchanged draining report did not commit its liveness: %v -> %v", before, after)
	}
	promoted := make(chan error, 1)
	go func() { promoted <- f.report(next.RuntimeProcessId, next.RegistrationReceipt, accepting) }()
	waitProcessSQLLock(ctx, t, admin, "ORDER BY registration_order FOR UPDATE")
	awaitBlocked(t, promoted, "promotion behind a held mutation")
	if _, _, _, owner := f.lifecycle(old); owner != old {
		t.Fatalf("current process %s before the held mutation committed", owner)
	}
	release()
	if err := <-written; err != nil {
		t.Fatalf("held draining mutation: %v", err)
	}
	if err := <-promoted; err != nil {
		t.Fatalf("promotion after the mutation: %v", err)
	}
	if _, current, retired, owner := f.lifecycle(old); current || !retired || owner != next.RuntimeProcessId {
		t.Fatalf("promotion did not retire the old process: current=%t retired=%t owner=%s", current, retired, owner)
	}
	if response, err := write("after-promotion"); err != nil || response.GetStale() == nil {
		t.Fatalf("retired process started new work: %v/%v", response, err)
	}
}

// A report whose liveness UPDATE snapshot predates a promotion or drain may be
// acknowledged, but it changes no lifecycle fact: the new process stays the
// sole current owner, the old one can neither mutate nor report again, and a
// draining process never returns to accepting. The overlapping report holds no
// process lock, so neither lifecycle change waits for it.
func TestPostgreSQLRuntimeReportOverlappingLifecycleChangeGrantsNoAuthority(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	f := newLivenessFixture(ctx, t, runtime, admin, "overlap")
	accepting := bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_ACCEPTING
	draining := bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_DRAINING
	old := f.scope.Binding.RuntimeProcessId
	next := f.register("overlap-next")
	// holdLiveness keeps a liveness row FOR SHARE, as a final loss
	// classification does, so the report's UPDATE waits after its snapshot.
	holdLiveness := func(processID string) *sql.Tx {
		t.Helper()
		hold, err := admin.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := hold.ExecContext(ctx, `SELECT 1 FROM runtime_process_liveness WHERE pod_uid=$1 AND runtime_process_id=$2 FOR SHARE`, f.scope.Binding.TargetPodUid, processID); err != nil {
			t.Fatal(err)
		}
		return hold
	}

	hold := holdLiveness(old)
	defer func() { _ = hold.Rollback() }()
	before := f.reportedAt(old)
	acknowledged := make(chan error, 1)
	go func() { acknowledged <- f.report(old, f.receipt, accepting) }()
	waitProcessSQLLock(ctx, t, admin, "UPDATE public.runtime_process_liveness AS live")
	if err := f.report(next.RuntimeProcessId, next.RegistrationReceipt, accepting); err != nil {
		t.Fatalf("promotion waited for or failed behind an overlapping report: %v", err)
	}
	awaitBlocked(t, acknowledged, "overlapping report")
	if err := hold.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-acknowledged; err != nil {
		t.Fatalf("overlapping report acknowledgment: %v", err)
	}
	if after := f.reportedAt(old); !after.Valid || !after.Time.After(before.Time) {
		t.Fatalf("overlapping report did not record its acknowledgment: %v -> %v", before, after)
	}
	if _, current, retired, owner := f.lifecycle(old); current || !retired || owner != next.RuntimeProcessId {
		t.Fatalf("acknowledged report changed lifecycle: current=%t retired=%t owner=%s", current, retired, owner)
	}
	if response, err := f.registry.WriteEvent(ctx, &bridgev1.WriteEventRequest{Scope: f.scope, RuntimeWriteId: "overlap-after-promotion", EventType: "session.status_running", PayloadJson: `{"type":"session.status_running"}`}); err != nil || response.GetStale() == nil {
		t.Fatalf("acknowledged retired process started new work: %v/%v", response, err)
	}
	if err := f.report(old, f.receipt, accepting); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("report after promotion: %v", err)
	}

	// A drain records its own report on the same liveness row, so it commits
	// its phase change in either order with the overlapping report.
	hold = holdLiveness(next.RuntimeProcessId)
	defer func() { _ = hold.Rollback() }()
	acknowledged = make(chan error, 1)
	go func() { acknowledged <- f.report(next.RuntimeProcessId, next.RegistrationReceipt, accepting) }()
	waitProcessSQLLock(ctx, t, admin, "UPDATE public.runtime_process_liveness AS live")
	drained := make(chan error, 1)
	go func() { drained <- f.report(next.RuntimeProcessId, next.RegistrationReceipt, draining) }()
	waitProcessSQLLock(ctx, t, admin, "UPDATE public.runtime_process_liveness SET reported_at")
	awaitBlocked(t, acknowledged, "overlapping accepting report")
	if err := hold.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-acknowledged; err != nil {
		t.Fatalf("overlapping accepting report acknowledgment: %v", err)
	}
	if err := <-drained; err != nil {
		t.Fatalf("drain overlapping an accepting report: %v", err)
	}
	if phase, current, retired, _ := f.lifecycle(next.RuntimeProcessId); phase != runtimecontrol.ProcessDraining || !current || retired {
		t.Fatalf("acknowledged report undid the drain: phase=%s current=%t retired=%t", phase, current, retired)
	}
	if err := f.report(next.RuntimeProcessId, next.RegistrationReceipt, accepting); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("draining process returned to accepting: %v", err)
	}
}

// The lifecycle path commits its process change and its report time together.
// A registered process without its liveness row is an invariant failure:
// registration retry and every report fail without promoting, changing phase
// or manufacturing the row.
func TestPostgreSQLRuntimeProcessLifecycleAndLivenessCommitTogether(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const podUID = "pod-liveness-atomic"
	f := &livenessFixture{t: t, ctx: ctx, admin: admin, scope: &bridgev1.RuntimeScope{Binding: &bridgev1.RuntimeBindingRef{TargetPodUid: podUID}}}
	f.registry = processRegistryRPCWithStore(t, livenessTestPolicy(NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))), podUID, nil)
	accepting := bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_ACCEPTING
	draining := bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_DRAINING
	registered := f.register("liveness-atomic")
	if reported := f.reportedAt(registered.RuntimeProcessId); reported.Valid {
		t.Fatalf("registration recorded a report: %v", reported)
	}
	livenessRows := func() int {
		t.Helper()
		var rows int
		if err := admin.QueryRow(`SELECT count(*) FROM runtime_process_liveness WHERE pod_uid=$1`, podUID).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		return rows
	}
	removeLiveness := func() {
		t.Helper()
		if _, err := admin.Exec(`DELETE FROM runtime_process_liveness WHERE pod_uid=$1 AND runtime_process_id=$2`, podUID, registered.RuntimeProcessId); err != nil {
			t.Fatal(err)
		}
	}
	restoreLiveness := func() {
		t.Helper()
		if _, err := admin.Exec(`INSERT INTO runtime_process_liveness(namespace,pod_uid,runtime_process_id) VALUES('tetral-agent-runtime',$1,$2)`, podUID, registered.RuntimeProcessId); err != nil {
			t.Fatal(err)
		}
	}
	requireRefused := func(state string) {
		t.Helper()
		if after := reportDeadlinePromotionSnapshot(t, admin, podUID); after != state {
			t.Fatalf("refused report changed lifecycle:\n%s\n%s", state, after)
		}
		if rows := livenessRows(); rows != 0 {
			t.Fatalf("refused report manufactured %d liveness rows", rows)
		}
	}

	removeLiveness()
	state := reportDeadlinePromotionSnapshot(t, admin, podUID)
	if _, err := f.registry.RegisterRuntimeProcess(ctx, &bridgev1.RegisterRuntimeProcessRequest{RuntimeProcessId: registered.RuntimeProcessId}); status.Code(err) != codes.Internal {
		t.Fatalf("registration retry without liveness: %v", err)
	}
	if err := f.report(registered.RuntimeProcessId, registered.RegistrationReceipt, accepting); status.Code(err) != codes.Internal {
		t.Fatalf("promotion without liveness: %v", err)
	}
	requireRefused(state)
	restoreLiveness()
	if err := f.report(registered.RuntimeProcessId, registered.RegistrationReceipt, accepting); err != nil {
		t.Fatal(err)
	}
	if phase, current, _, _ := f.lifecycle(registered.RuntimeProcessId); phase != runtimecontrol.ProcessAccepting || !current || !f.reportedAt(registered.RuntimeProcessId).Valid {
		t.Fatalf("promotion committed without its report: phase=%s current=%t", phase, current)
	}

	removeLiveness()
	state = reportDeadlinePromotionSnapshot(t, admin, podUID)
	for name, phase := range map[string]bridgev1.RuntimeProcessPhase{"unchanged report": accepting, "phase change": draining} {
		if err := f.report(registered.RuntimeProcessId, registered.RegistrationReceipt, phase); status.Code(err) != codes.Internal {
			t.Fatalf("%s without liveness: %v", name, err)
		}
	}
	requireRefused(state)
	restoreLiveness()
	if err := f.report(registered.RuntimeProcessId, registered.RegistrationReceipt, draining); err != nil {
		t.Fatal(err)
	}
	if phase, current, _, _ := f.lifecycle(registered.RuntimeProcessId); phase != runtimecontrol.ProcessDraining || !current || !f.reportedAt(registered.RuntimeProcessId).Valid {
		t.Fatalf("phase change committed without its report: phase=%s current=%t", phase, current)
	}
}

// A committed promotion wakes Job Runner acquisition and, separately, requests
// a Pod-loss repair run for the process it replaced.
func TestPostgreSQLRuntimeProcessPromotionWakesRunnerAndRequestsRepair(t *testing.T) {
	runtime, _ := storagetest.NewPostgreSQLDBWithAdmin(t)
	client := dbconnect.NewClientForTesting(runtime)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	payloads := make(chan string, 16)
	ready := make(chan struct{})
	var readyOnce sync.Once
	listenCtx, stopListening := context.WithCancel(ctx)
	listened := make(chan error, 1)
	go func() {
		listened <- (queue.PostgreSQLNotificationListener{Client: client}).Listen(listenCtx, queue.NotificationChannel,
			func() { readyOnce.Do(func() { close(ready) }) }, func(payload string) { payloads <- payload })
	}()
	defer func() {
		stopListening()
		<-listened
	}()
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal("notification listener did not become ready")
	}
	rpc := processRegistryRPC(t, client, "pod-promotion-wake")
	accepting := bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_ACCEPTING
	for _, processID := range []string{"process-promotion-first", "process-promotion-takeover"} {
		registered, err := rpc.RegisterRuntimeProcess(ctx, &bridgev1.RegisterRuntimeProcessRequest{RuntimeProcessId: processID})
		if err != nil {
			t.Fatal(err)
		}
		response, err := rpc.ReportRuntimeProcess(ctx, &bridgev1.ReportRuntimeProcessRequest{RuntimeProcessId: processID, RegistrationReceipt: registered.RegistrationReceipt, Phase: accepting})
		if err != nil || !response.GetCurrent() {
			t.Fatalf("promote %s = %+v/%v", processID, response, err)
		}
		received := map[string]int{}
		for len(received) < 2 {
			select {
			case payload := <-payloads:
				received[payload]++
			case <-ctx.Done():
				t.Fatalf("promotion of %s notified %v; want Job Runner wake and repair request", processID, received)
			}
		}
		if received[queue.ConsumerClassJobRunner] != 1 || received[queue.NotificationClassRuntimeProcess] != 1 {
			t.Fatalf("promotion of %s notified %v; want one Job Runner wake and one repair request", processID, received)
		}
	}
}
