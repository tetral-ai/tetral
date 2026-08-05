package tetralqueue

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/internalgrpc"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sessionrpc"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

func TestQueueServerLogsSuccessfulLeaseWaitAndDurableIdentity(t *testing.T) {
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	store := &recordingStore{leaseJobs: []*queue.Job{{
		ID: "qjob_observed", WorkspaceID: "ws_observed", Kind: queue.KindSandboxToolExecute,
		PartitionKey: "sandbox:ws_observed:sesn_observed", CreatedAt: now.Add(-time.Hour),
		AvailableAt: now.Add(-500 * time.Millisecond), AttemptCount: 2,
	}}}
	var logs bytes.Buffer
	server := NewServer(store, slog.New(slog.NewJSONHandler(&logs, nil)))
	nowCalls := 0
	server.now = func() time.Time {
		nowCalls++
		if nowCalls == 1 {
			return now
		}
		return now.Add(25 * time.Millisecond)
	}
	if _, err := server.Lease(context.Background(), &queuev1.LeaseRequest{
		WorkspaceId: "ws_observed", Kinds: []string{queue.KindSandboxToolExecute},
		LeaseOwner: "sandbox", MaxJobs: 1, LeaseDurationMs: 1000,
	}); err != nil {
		t.Fatalf("Lease: %v", err)
	}
	for _, want := range []string{
		`"msg":"queue.job.leased"`, `"workspace.id":"ws_observed"`,
		`"queue.job.id":"qjob_observed"`, `"queue.job.kind":"sandbox_tool_execute"`,
		`"duration.ms":25`, `"queue.ready_wait.ms":500`,
	} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("lease log missing %s: %s", want, logs.String())
		}
	}
	if strings.Contains(logs.String(), "queue.age.ms") {
		t.Fatalf("lease log must not contain queue.age.ms: %s", logs.String())
	}
}

func TestQueueServiceGeneratedClientLeasesAndFencesTransitions(t *testing.T) {
	now := time.Date(2026, 7, 1, 16, 0, 0, 0, time.UTC)
	store := &recordingStore{
		leaseJobs: []*queue.Job{{
			ID:             "qjob_rpc",
			WorkspaceID:    workspace.ID("ws_queue_rpc"),
			Kind:           queue.KindRuntimeInput,
			PartitionKey:   "session:ws_queue_rpc:sesn_rpc",
			DedupeKey:      "runtime_input:ws_queue_rpc:sesn_rpc:input_1",
			PayloadVersion: 1,
			PayloadJSON:    []byte(`{"runtime_input_id":"input_1"}`),
			Status:         queue.StatusLeased,
			Priority:       100,
			AvailableAt:    now,
			LeasedBy:       "bridge",
			LeaseToken:     "qlt_rpc",
			LeasedAt:       &now,
			LeasedUntil:    timePtr(now.Add(time.Minute)),
			AttemptCount:   1,
			MaxAttempts:    10,
		}},
	}
	conn, cleanup := newQueueClientConn(t, store, now)
	defer cleanup()
	client := queuev1.NewQueueServiceClient(conn)

	lease, err := client.Lease(context.Background(), &queuev1.LeaseRequest{
		WorkspaceId:     "ws_queue_rpc",
		Kinds:           []string{queue.KindRuntimeInput, queue.KindRuntimeConfigUpdate, queue.KindCleanupSession},
		LeaseOwner:      "bridge",
		MaxJobs:         1,
		LeaseDurationMs: 60000,
	})
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if len(lease.GetJobs()) != 1 || lease.GetJobs()[0].GetId() != "qjob_rpc" || lease.GetJobs()[0].GetPayloadJson() == "" {
		t.Fatalf("Lease response = %#v; want one durable-reference job", lease.GetJobs())
	}
	if store.leaseRequest.LeaseDuration != time.Minute || store.leaseRequest.Now != now {
		t.Fatalf("stored lease request = %#v; want duration/now from service", store.leaseRequest)
	}
	if len(store.leaseRequest.Kinds) != 3 || store.leaseRequest.Kinds[1] != queue.KindRuntimeConfigUpdate {
		t.Fatalf("stored lease kinds = %v; want runtime-facing kind set", store.leaseRequest.Kinds)
	}

	ack, err := client.Ack(context.Background(), &queuev1.AckRequest{
		WorkspaceId: "ws_queue_rpc",
		JobId:       "qjob_rpc",
		LeaseToken:  "stale-token",
	})
	if err != nil {
		t.Fatalf("Ack stale: %v", err)
	}
	if ack.GetUpdated() {
		t.Fatal("stale Ack response updated=true; want false")
	}
	if store.ackRequest.LeaseToken != "stale-token" || store.ackRequest.Now != now {
		t.Fatalf("stored ack request = %#v; want fenced token and service time", store.ackRequest)
	}

	cancel, err := client.Cancel(context.Background(), &queuev1.CancelRequest{
		WorkspaceId:            "ws_queue_rpc",
		SessionId:              "sesn_rpc",
		SessionThreadId:        "thrd_rpc",
		InterruptFenceSequence: 17,
	})
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if cancel.GetCancelledCount() != 2 {
		t.Fatalf("Cancel count = %d; want 2", cancel.GetCancelledCount())
	}
	if store.cancelRequest.SessionID != "sesn_rpc" || store.cancelRequest.InterruptFenceSequence != 17 || store.cancelRequest.Now != now {
		t.Fatalf("stored cancel request = %#v; want interrupt fence fields and service time", store.cancelRequest)
	}

	deferred, err := client.Defer(context.Background(), &queuev1.DeferRequest{
		WorkspaceId: "ws_queue_rpc",
		JobId:       "qjob_rpc",
		LeaseToken:  "qlt_rpc",
	})
	if err != nil {
		t.Fatalf("Defer: %v", err)
	}
	if !deferred.GetUpdated() {
		t.Fatal("Defer response updated=false; want true")
	}
	if store.deferRequest.JobID != "qjob_rpc" || store.deferRequest.LeaseToken != "qlt_rpc" || store.deferRequest.Now != now {
		t.Fatalf("stored defer request = %#v; want fenced token and service time", store.deferRequest)
	}
}

func TestQueueLeaseCarriesMaximumLegalBatchWithinScopedFuse(t *testing.T) {
	now := time.Date(294276, 12, 31, 23, 59, 59, 999999999, time.UTC)
	const payloadPrefix = `{"payload":"`
	const payloadSuffix = `"}`
	maxPayload := []byte(payloadPrefix + strings.Repeat("x", queue.MaxQueueJobPayloadBytes-len(payloadPrefix)-len(payloadSuffix)) + payloadSuffix)
	if len(maxPayload) != queue.MaxQueueJobPayloadBytes {
		t.Fatalf("maximum payload bytes = %d; want %d", len(maxPayload), queue.MaxQueueJobPayloadBytes)
	}
	jobs := make([]*queue.Job, 0, queue.MaxQueueLeaseJobs())
	for index := 0; index < queue.MaxQueueLeaseJobs(); index++ {
		suffix := fmt.Sprintf("%016x", index)
		jobs = append(jobs, &queue.Job{
			ID:             queue.JobIDPrefix + suffix,
			WorkspaceID:    workspace.ID(strings.Repeat("w", workspace.MaxWorkspaceIDBytes)),
			Kind:           queue.KindSandboxToolExecute,
			PartitionKey:   strings.Repeat("p", queue.MaxQueuePartitionKeyBytes-len(suffix)) + suffix,
			DedupeKey:      strings.Repeat("d", queue.MaxQueueDedupeKeyBytes-len(suffix)) + suffix,
			PayloadVersion: int(^uint32(0) >> 1),
			PayloadJSON:    append([]byte(nil), maxPayload...),
			Status:         queue.StatusLeased,
			Priority:       int(^uint32(0) >> 1),
			AvailableAt:    now,
			LeasedBy:       strings.Repeat("l", queue.MaxQueueLeaseOwnerBytes),
			LeaseToken:     "qlt_" + strings.Repeat("a", queue.MaxQueueLeaseTokenBytes-len("qlt_")-len(suffix)) + suffix,
			LeasedAt:       &now,
			LeasedUntil:    timePtr(now.Add(time.Minute)),
			AttemptCount:   int(^uint32(0) >> 1),
			MaxAttempts:    int(^uint32(0) >> 1),
		})
	}
	store := &recordingStore{leaseJobs: jobs}
	conn, cleanup := newQueueClientConn(t, store, now)
	defer cleanup()

	response, err := queuev1.NewQueueServiceClient(conn).Lease(context.Background(), &queuev1.LeaseRequest{
		WorkspaceId: strings.Repeat("w", workspace.MaxWorkspaceIDBytes), Kinds: []string{queue.KindSandboxToolExecute},
		LeaseOwner: strings.Repeat("l", queue.MaxQueueLeaseOwnerBytes), MaxJobs: int32(queue.MaxQueueLeaseJobs()), LeaseDurationMs: 60000,
	})
	if err != nil {
		t.Fatalf("Lease maximum legal batch: %v", err)
	}
	if got := len(response.GetJobs()); got != queue.MaxQueueLeaseJobs() {
		t.Fatalf("leased jobs = %d; want derived maximum %d", got, queue.MaxQueueLeaseJobs())
	}
	if got := proto.Size(response); got > sessionrpc.MaxQueueLeaseGRPCMessageBytes {
		t.Fatalf("serialized Lease response = %d bytes; exceeds scoped %d-byte fuse", got, sessionrpc.MaxQueueLeaseGRPCMessageBytes)
	}
}

func TestQueueJobVariableFieldCensusMatchesLeaseArithmetic(t *testing.T) {
	bounds := queue.QueueJobFieldBounds()
	bounded := make(map[string]queue.QueueJobFieldBound, len(bounds))
	derivedEnvelope := 4 // LeaseResponse repeated-field tag plus encoded job length.
	for _, bound := range bounds {
		if bound.MaxValueBytes <= 0 {
			t.Fatalf("QueueJob field %q has non-positive bound %d", bound.Name, bound.MaxValueBytes)
		}
		if _, duplicate := bounded[bound.Name]; duplicate {
			t.Fatalf("QueueJob field %q is registered twice", bound.Name)
		}
		bounded[bound.Name] = bound
		derivedEnvelope += queue.QueueJobFieldEnvelopeBytes(bound)
	}
	fields := (&queuev1.QueueJob{}).ProtoReflect().Descriptor().Fields()
	if fields.Len() != len(bounded) {
		t.Fatalf("QueueJob fields = %d; registered bounds = %d", fields.Len(), len(bounded))
	}
	for index := 0; index < fields.Len(); index++ {
		name := string(fields.Get(index).Name())
		if _, ok := bounded[name]; !ok {
			t.Fatalf("QueueJob field %q has no positive registered bound", name)
		}
	}
	if derivedEnvelope != queue.QueueJobEnvelopeAllowance() {
		t.Fatalf("QueueJob envelope from registered fields = %d; production arithmetic = %d", derivedEnvelope, queue.QueueJobEnvelopeAllowance())
	}
	payloadBound, ok := bounded["payload_json"]
	if !ok || !payloadBound.SeparatePayload || payloadBound.MaxValueBytes != queue.MaxQueueJobPayloadBytes {
		t.Fatalf("payload_json registry = %+v; want separately charged %d-byte payload", payloadBound, queue.MaxQueueJobPayloadBytes)
	}
}

func TestQueueServiceValidationErrorsMapToInvalidArgument(t *testing.T) {
	conn, cleanup := newQueueClientConn(t, &recordingStore{}, time.Now())
	defer cleanup()
	client := queuev1.NewQueueServiceClient(conn)

	_, err := client.Lease(context.Background(), &queuev1.LeaseRequest{
		WorkspaceId:     "ws_queue_rpc",
		Kinds:           []string{queue.KindRuntimeInput},
		LeaseOwner:      "bridge",
		MaxJobs:         1,
		LeaseDurationMs: 0,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Lease invalid duration error = %v; want InvalidArgument", err)
	}
}

func TestPositiveMillisDurationRejectsOverflow(t *testing.T) {
	maxSafeMillis := int64(math.MaxInt64) / int64(time.Millisecond)
	if got, err := positiveMillisDuration(maxSafeMillis, "lease_duration_ms"); err != nil || got != time.Duration(maxSafeMillis)*time.Millisecond {
		t.Fatalf("largest safe duration = %s, %v; want exact conversion", got, err)
	}
	if _, err := positiveMillisDuration(maxSafeMillis+1, "lease_duration_ms"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("overflow duration error = %v; want InvalidArgument", err)
	}
	if _, err := positiveMillisDuration(maxSafeMillis+1, "heartbeat_lease_duration_ms"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("heartbeat overflow duration error = %v; want InvalidArgument", err)
	}
}

func newQueueClientConn(t *testing.T, store *recordingStore, now time.Time) (*grpc.ClientConn, func()) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(internalgrpc.QueueRPCServerOptions()...)
	queueServer := NewServer(store, nil)
	queueServer.now = func() time.Time { return now }
	queuev1.RegisterQueueServiceServer(server, queueServer)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	dialOptions := append([]grpc.DialOption{}, internalgrpc.QueueRPCDialOptions()...)
	dialOptions = append(dialOptions,
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	conn, err := grpc.NewClient("passthrough:///bufnet", dialOptions...)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return conn, func() {
		_ = conn.Close()
		server.Stop()
		if err := <-done; err != nil && !strings.Contains(err.Error(), "closed") {
			t.Fatalf("queue gRPC server: %v", err)
		}
	}
}

type recordingStore struct {
	leaseJobs     []*queue.Job
	leaseRequest  queue.LeaseRequest
	ackRequest    queue.AckRequest
	deferRequest  queue.DeferRequest
	cancelRequest queue.CancelRequest
}

func (s *recordingStore) Lease(_ context.Context, request queue.LeaseRequest) ([]*queue.Job, error) {
	s.leaseRequest = request
	return s.leaseJobs, nil
}

func (s *recordingStore) Heartbeat(context.Context, queue.HeartbeatRequest) (queue.HeartbeatResult, error) {
	return queue.HeartbeatResult{Updated: true, LeasedUntil: time.Now().UTC().Add(time.Minute)}, nil
}

func (s *recordingStore) Ack(_ context.Context, request queue.AckRequest) (bool, error) {
	s.ackRequest = request
	return false, nil
}

func (s *recordingStore) Retry(context.Context, queue.RetryRequest) (bool, error) {
	return true, nil
}

func (s *recordingStore) Defer(_ context.Context, request queue.DeferRequest) (bool, error) {
	s.deferRequest = request
	return true, nil
}

func (s *recordingStore) DeadLetter(context.Context, queue.DeadLetterRequest) (bool, error) {
	return true, nil
}

func (s *recordingStore) Cancel(_ context.Context, request queue.CancelRequest) (int, error) {
	s.cancelRequest = request
	return 2, nil
}

func timePtr(value time.Time) *time.Time {
	return &value
}
