package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

// Direct addresses intentionally prove shared durable authority across replicas;
// they make no claim about Envoy endpoint selection.
func TestPostgreSQLReplicaQueueReceipts(t *testing.T) {
	for _, scenario := range []string{"cross_replica_transitions", "lost_lease_response", "lost_ack_response"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			var stores [3]*queue.PostgreSQLQueueStore
			var clients [3]queuev1.QueueServiceClient
			fault := &queueReplicaResponseFault{}
			for i := range stores {
				pool := storagetest.OpenRuntimeRoleDBWithTracer(t, runtimeDB, nil)
				stores[i] = queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(pool))
				clients[i] = serveQueueReplica(t, stores[i], fault)
			}
			first := enqueueReplicaQueueConfig(t, stores[0], "default", "replica", 1)
			switch scenario {
			case "cross_replica_transitions":
				second := enqueueReplicaQueueConfig(t, stores[0], "default", "replica", 2)
				other := enqueueReplicaQueueConfig(t, stores[0], "other-workspace", "isolated", 1)
				leased := leaseReplicaQueue(ctx, t, clients[0], time.Second)
				if len(leased) != 1 || leased[0].GetId() != first {
					t.Fatal("first replica did not acquire original job")
				}
				hb, err := clients[1].Heartbeat(ctx, &queuev1.HeartbeatRequest{WorkspaceId: "default", JobId: first, LeaseToken: leased[0].GetLeaseToken(), LeaseDurationMs: 1000})
				if err != nil || !hb.GetUpdated() {
					t.Fatalf("cross-replica heartbeat=%v/%v", hb, err)
				}
				if jobs := leaseReplicaQueue(ctx, t, clients[2], time.Second); len(jobs) != 0 {
					t.Fatal("another replica crossed the Session-exclusive lease barrier")
				}
				denied, err := clients[2].Ack(ctx, &queuev1.AckRequest{WorkspaceId: "other-workspace", JobId: first, LeaseToken: leased[0].GetLeaseToken()})
				if err != nil || denied.GetUpdated() {
					t.Fatalf("cross-workspace acknowledgement=%v/%v", denied, err)
				}
				ackReplicaQueue(ctx, t, clients[2], leased[0], true)
				next := leaseReplicaQueue(ctx, t, clients[1], time.Second)
				if len(next) != 1 || next[0].GetId() != second {
					t.Fatal("next Session position did not become available after committed ACK")
				}
				ackReplicaQueue(ctx, t, clients[0], next[0], true)
				assertReplicaQueueState(ctx, t, admin, "default", first, queue.StatusAcknowledged)
				assertReplicaQueueState(ctx, t, admin, "other-workspace", other, queue.StatusPending)
			case "lost_lease_response":
				cut := fault.arm("Lease")
				lostCtx, stop := context.WithCancel(ctx)
				defer stop()
				done := make(chan error, 1)
				go func() { _, err := clients[0].Lease(lostCtx, replicaLeaseRequest(300*time.Millisecond)); done <- err }()
				awaitReplicaQueueBarrier(ctx, t, cut, "committed Lease response")
				var oldToken string
				if err := admin.QueryRowContext(ctx, `SELECT lease_token FROM queue_jobs WHERE workspace_id='default' AND id=$1 AND status='leased'`, first).Scan(&oldToken); err != nil || oldToken == "" {
					t.Fatalf("lost Lease did not commit one durable token: %v", err)
				}
				if next := leaseReplicaQueue(ctx, t, clients[1], time.Second); len(next) != 0 {
					t.Fatal("unseen lease was redelivered before expiry")
				}
				stop()
				awaitReplicaQueueError(ctx, t, done)
				awaitReplicaQueueExpiry(ctx, t, admin, first)
				reclaimed, err := stores[2].ReclaimExpiredLeases(ctx, queue.ReclaimExpiredLeasesRequest{Limit: 10})
				if err != nil || reclaimed != 1 {
					t.Fatalf("replacement reclaim=%d/%v", reclaimed, err)
				}
				next := leaseReplicaQueue(ctx, t, clients[1], time.Second)
				if len(next) != 1 || next[0].GetId() != first || next[0].GetLeaseToken() == oldToken {
					t.Fatal("reclaim did not issue a fresh token for the same job")
				}
				assertReplicaQueueStaleToken(ctx, t, clients, first, oldToken)
				var currentToken string
				if err := admin.QueryRowContext(ctx, `SELECT lease_token FROM queue_jobs WHERE workspace_id='default' AND id=$1 AND status='leased'`, first).Scan(&currentToken); err != nil || currentToken != next[0].GetLeaseToken() {
					t.Fatalf("stale calls changed successor custody: %v", err)
				}
				ackReplicaQueue(ctx, t, clients[2], next[0], true)
			case "lost_ack_response":
				leased := leaseReplicaQueue(ctx, t, clients[0], time.Second)
				if len(leased) != 1 {
					t.Fatal("missing original lease")
				}
				cut := fault.arm("Ack")
				lostCtx, stop := context.WithCancel(ctx)
				defer stop()
				done := make(chan error, 1)
				go func() {
					_, err := clients[1].Ack(lostCtx, &queuev1.AckRequest{WorkspaceId: "default", JobId: first, LeaseToken: leased[0].GetLeaseToken()})
					done <- err
				}()
				awaitReplicaQueueBarrier(ctx, t, cut, "committed Ack response")
				assertReplicaQueueState(ctx, t, admin, "default", first, queue.StatusAcknowledged)
				stop()
				awaitReplicaQueueError(ctx, t, done)
				ackReplicaQueue(ctx, t, clients[2], leased[0], false)
				if n, err := stores[0].ReclaimExpiredLeases(ctx, queue.ReclaimExpiredLeasesRequest{Limit: 10}); err != nil || n != 0 {
					t.Fatalf("lost ACK resurrected completed work: %d/%v", n, err)
				}
				if next := leaseReplicaQueue(ctx, t, clients[2], time.Second); len(next) != 0 {
					t.Fatal("completed job leased again after ACK response loss")
				}
			}
		})
	}
}

type queueReplicaFault struct {
	method    string
	committed chan struct{}
}
type queueReplicaResponseFault struct {
	next atomic.Pointer[queueReplicaFault]
}

func (f *queueReplicaResponseFault) arm(method string) <-chan struct{} {
	cut := &queueReplicaFault{method: method, committed: make(chan struct{})}
	f.next.Store(cut)
	return cut.committed
}
func (f *queueReplicaResponseFault) intercept(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	response, err := handler(ctx, request)
	cut := f.next.Load()
	if err == nil && cut != nil && strings.HasSuffix(info.FullMethod, "/"+cut.method) && f.next.CompareAndSwap(cut, nil) {
		close(cut.committed)
		<-ctx.Done()
		return nil, status.Error(codes.Canceled, "fixture dropped committed response")
	}
	return response, err
}

func serveQueueReplica(t *testing.T, store tetralqueue.Store, fault *queueReplicaResponseFault) queuev1.QueueServiceClient {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var options []grpc.ServerOption
	if fault != nil {
		options = append(options, grpc.UnaryInterceptor(fault.intercept))
	}
	server := grpc.NewServer(options...)
	tetralqueue.Register(server, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	joined := make(chan struct{})
	go func() { defer close(joined); _ = server.Serve(listener) }()
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, conn *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		started := time.Now()
		err := invoke(ctx, method, req, reply, conn, opts...)
		replicaRecordCompletion(t, "ordinary_short_rpc", method, listener.Addr().String(), status.Code(err).String(), started)
		return err
	}))
	if err != nil {
		server.Stop()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(); server.Stop(); _ = listener.Close(); <-joined })
	return queuev1.NewQueueServiceClient(conn)
}
func enqueueReplicaQueueConfig(t *testing.T, store *queue.PostgreSQLQueueStore, ws, suffix string, generation int) string {
	t.Helper()
	sessionID := "sesn_" + suffix
	payload, err := json.Marshal(map[string]any{"workspace_id": ws, "session_id": sessionID, "config_generation": generation})
	if err != nil {
		t.Fatal(err)
	}
	job, err := store.Enqueue(context.Background(), queue.EnqueueRequest{WorkspaceID: workspace.ID(ws), Kind: queue.KindRuntimeConfigUpdate, PartitionKey: queue.FormatSessionPartitionKey(workspace.ID(ws), sessionID), DedupeKey: queue.FormatRuntimeConfigUpdateDedupeKey(workspace.ID(ws), sessionID, strconv.Itoa(generation)), PayloadJSON: payload, Now: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	return job.ID
}
func replicaLeaseRequest(duration time.Duration) *queuev1.LeaseRequest {
	return &queuev1.LeaseRequest{WorkspaceId: "default", Kinds: []string{queue.KindRuntimeConfigUpdate}, LeaseOwner: "replica-fixture", MaxJobs: 10, LeaseDurationMs: duration.Milliseconds()}
}
func leaseReplicaQueue(ctx context.Context, t *testing.T, c queuev1.QueueServiceClient, duration time.Duration) []*queuev1.QueueJob {
	t.Helper()
	r, err := c.Lease(ctx, replicaLeaseRequest(duration))
	if err != nil {
		t.Fatal(err)
	}
	return r.GetJobs()
}
func ackReplicaQueue(ctx context.Context, t *testing.T, c queuev1.QueueServiceClient, job *queuev1.QueueJob, want bool) {
	t.Helper()
	r, err := c.Ack(ctx, &queuev1.AckRequest{WorkspaceId: job.GetWorkspaceId(), JobId: job.GetId(), LeaseToken: job.GetLeaseToken()})
	if err != nil || r.GetUpdated() != want {
		t.Fatalf("ACK updated=%t/%v; want %t", r.GetUpdated(), err, want)
	}
}
func awaitReplicaQueueBarrier(ctx context.Context, t *testing.T, ch <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatalf("%s: %v", description, ctx.Err())
	}
}
func awaitReplicaQueueError(ctx context.Context, t *testing.T, ch <-chan error) {
	t.Helper()
	select {
	case err := <-ch:
		if err == nil {
			t.Fatal("response-loss caller unexpectedly succeeded")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
func assertReplicaQueueState(ctx context.Context, t *testing.T, db *sql.DB, ws, id, want string) {
	t.Helper()
	var got string
	if err := db.QueryRowContext(ctx, `SELECT status FROM queue_jobs WHERE workspace_id=$1 AND id=$2`, ws, id).Scan(&got); err != nil || got != want {
		t.Fatalf("durable Queue state=%s/%v; want %s", got, err, want)
	}
}
func awaitReplicaQueueExpiry(ctx context.Context, t *testing.T, db *sql.DB, id string) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var expired bool
		if err := db.QueryRowContext(ctx, `SELECT leased_until <= clock_timestamp() FROM queue_jobs WHERE workspace_id='default' AND id=$1`, id).Scan(&expired); err != nil {
			t.Fatal(err)
		}
		if expired {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}
func assertReplicaQueueStaleToken(ctx context.Context, t *testing.T, clients [3]queuev1.QueueServiceClient, id, token string) {
	t.Helper()
	hb, err := clients[0].Heartbeat(ctx, &queuev1.HeartbeatRequest{WorkspaceId: "default", JobId: id, LeaseToken: token, LeaseDurationMs: 1000})
	if err != nil || hb.GetUpdated() {
		t.Fatalf("stale Heartbeat=%v/%v", hb, err)
	}
	calls := []struct {
		name string
		call func() (*queuev1.TransitionResponse, error)
	}{
		{"Ack", func() (*queuev1.TransitionResponse, error) {
			return clients[1].Ack(ctx, &queuev1.AckRequest{WorkspaceId: "default", JobId: id, LeaseToken: token})
		}},
		{"Retry", func() (*queuev1.TransitionResponse, error) {
			return clients[2].Retry(ctx, &queuev1.RetryRequest{WorkspaceId: "default", JobId: id, LeaseToken: token, ErrorKind: "fixture", ErrorMessage: "fixture"})
		}},
		{"Defer", func() (*queuev1.TransitionResponse, error) {
			return clients[0].Defer(ctx, &queuev1.DeferRequest{WorkspaceId: "default", JobId: id, LeaseToken: token})
		}},
		{"DeadLetter", func() (*queuev1.TransitionResponse, error) {
			return clients[1].DeadLetter(ctx, &queuev1.DeadLetterRequest{WorkspaceId: "default", JobId: id, LeaseToken: token, ErrorKind: "fixture", ErrorMessage: "fixture"})
		}},
	}
	for _, call := range calls {
		r, err := call.call()
		if err != nil || r.GetUpdated() {
			t.Fatalf("stale %s=%v/%v", call.name, r, err)
		}
	}
}
