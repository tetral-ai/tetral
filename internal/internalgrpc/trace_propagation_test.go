package internalgrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/tetral-ai/tetral/internal/workload"
)

const sessionMethod = "/tetral.test.v1.SessionService/Call"

const sessionStreamMethod = "/tetral.test.v1.SessionService/Stream"

// sessionRequestType is a request message with session_id, workspace_id and
// thread_id fields, as the Session RPCs carry.
func sessionRequestType(t *testing.T) protoreflect.MessageType {
	t.Helper()
	var fields []*descriptorpb.FieldDescriptorProto
	for number, name := range []string{"session_id", "workspace_id", "thread_id"} {
		fields = append(fields, &descriptorpb.FieldDescriptorProto{
			Name: new(name), Number: new(int32(number + 1)),
			Type:  descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		})
	}
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:        new("session_test.proto"),
		Package:     new("tetral.test.v1"),
		Syntax:      new("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{Name: new("SessionRequest"), Field: fields}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return dynamicpb.NewMessageType(file.Messages().ByName("SessionRequest"))
}

type lockedBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}

func TestInternalCallKeepsOneTraceAcrossClientAndServerAndRecordsTheRequestSession(t *testing.T) {
	spans := tracetest.NewInMemoryExporter()
	t.Cleanup(workload.InstallTelemetry(workload.NewTelemetry(workload.TelemetryOptions{Service: "queue", SampleRatio: 1, SyncExporter: spans})))
	requestType := sessionRequestType(t)
	var records lockedBuffer
	debug := workload.DefaultDiagnosticConfig()
	debug.Level = slog.LevelDebug
	owner := workload.NewProcessLogger(&records, "queue", "test", "unit", debug)
	logger := owner.Logger
	metrics := workload.NewGRPCMetrics("queue")
	serverTraceparent := make(chan string, 1)
	listener := bufconn.Listen(1024 * 1024)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Config{ServiceName: "queue", Authenticator: &allowingAuthenticator{}, Metrics: metrics, Logger: logger, Listener: listener,
			Register: func(server *grpc.Server) {
				server.RegisterService(&grpc.ServiceDesc{
					ServiceName: "tetral.test.v1.SessionService",
					HandlerType: (*testService)(nil),
					Methods: []grpc.MethodDesc{{MethodName: "Call", Handler: func(_ any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
						request := requestType.New().Interface()
						if err := decode(request); err != nil {
							return nil, err
						}
						return interceptor(ctx, request, &grpc.UnaryServerInfo{FullMethod: sessionMethod}, func(ctx context.Context, _ any) (any, error) {
							serverTraceparent <- workload.ContextTraceparent(ctx)
							return requestType.New().Interface(), nil
						})
					}}},
				}, struct{}{})
			}})
	}()
	conn, err := grpc.NewClient("passthrough:///bufnet", append(SessionRPCDialOptions(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		t.Fatal(err)
	}
	callerCtx, span := workload.StartSpan(context.Background(), "caller")
	callerTraceparent := workload.ContextTraceparent(callerCtx)
	request := requestType.New()
	request.Set(requestType.Descriptor().Fields().ByName("session_id"), protoreflect.ValueOfString("sesn_trace"))
	callerCtx = metadata.AppendToOutgoingContext(callerCtx, "authorization", "Bearer trace-test")
	if err := conn.Invoke(callerCtx, sessionMethod, request.Interface(), requestType.New().Interface()); err != nil {
		t.Fatalf("call: %v", err)
	}
	span.End()
	_ = conn.Close()
	cancel()
	<-done
	owner.CloseWithBudget()
	got := <-serverTraceparent
	if got[3:35] != callerTraceparent[3:35] {
		t.Fatalf("server trace %s differs from the caller's %s", got[3:35], callerTraceparent[3:35])
	}
	var record map[string]any
	for _, line := range strings.Split(strings.TrimSpace(records.String()), "\n") {
		var candidate map[string]any
		if json.Unmarshal([]byte(line), &candidate) == nil && candidate["msg"] == "internal.grpc.request" {
			record = candidate
		}
	}
	if record == nil || record["session.id"] != "sesn_trace" || record["level"] != "DEBUG" {
		t.Fatalf("server record = %v; want a DEBUG record with the request's session.id", record)
	}
	var serverSpanSession string
	for _, exported := range spans.GetSpans() {
		if exported.Name != sessionMethod {
			continue
		}
		for _, attribute := range exported.Attributes {
			if attribute.Key == "session.id" {
				serverSpanSession = attribute.Value.AsString()
			}
		}
	}
	if serverSpanSession != "sesn_trace" {
		t.Fatalf("server span session.id = %q; want the request's sesn_trace", serverSpanSession)
	}
	assertGRPCMetricCount(t, metrics, sessionMethod, "OK", 1)
}

// A stream call has no request until its first message arrives; the
// interceptor takes the correlation fields from that message.
func TestInternalStreamCarriesTheCorrelationFieldsOfItsFirstMessage(t *testing.T) {
	spans := tracetest.NewInMemoryExporter()
	t.Cleanup(workload.InstallTelemetry(workload.NewTelemetry(workload.TelemetryOptions{Service: "queue", SampleRatio: 1, SyncExporter: spans})))
	requestType := sessionRequestType(t)
	var records lockedBuffer
	debug := workload.DefaultDiagnosticConfig()
	debug.Level = slog.LevelDebug
	owner := workload.NewProcessLogger(&records, "queue", "test", "unit", debug)
	handlerFields := make(chan []slog.Attr, 1)
	listener := bufconn.Listen(1024 * 1024)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Config{ServiceName: "queue", Authenticator: &allowingAuthenticator{}, Logger: owner.Logger, Listener: listener,
			Register: func(server *grpc.Server) {
				server.RegisterService(&grpc.ServiceDesc{
					ServiceName: "tetral.test.v1.SessionService",
					HandlerType: (*testService)(nil),
					Streams: []grpc.StreamDesc{{StreamName: "Stream", ClientStreams: true, Handler: func(_ any, stream grpc.ServerStream) error {
						if err := stream.RecvMsg(requestType.New().Interface()); err != nil {
							return err
						}
						handlerFields <- workload.ContextFields(stream.Context())
						return stream.SendMsg(requestType.New().Interface())
					}}},
				}, struct{}{})
			}})
	}()
	conn, err := grpc.NewClient("passthrough:///bufnet", append(SessionRPCDialOptions(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"workspace.id": "ws_stream", "session.id": "sesn_stream", "thread.id": "thrd_stream"}
	request := requestType.New()
	for name, value := range map[protoreflect.Name]string{"workspace_id": "ws_stream", "session_id": "sesn_stream", "thread_id": "thrd_stream"} {
		request.Set(requestType.Descriptor().Fields().ByName(name), protoreflect.ValueOfString(value))
	}
	callerCtx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer trace-test")
	stream, err := conn.NewStream(callerCtx, &grpc.StreamDesc{StreamName: "Stream", ClientStreams: true}, sessionStreamMethod)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.SendMsg(request.Interface()); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if err := stream.RecvMsg(requestType.New().Interface()); err != nil {
		t.Fatalf("stream: %v", err)
	}
	_ = conn.Close()
	cancel()
	<-done
	owner.CloseWithBudget()
	got := map[string]string{}
	for _, field := range <-handlerFields {
		got[field.Key] = field.Value.String()
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("handler context fields = %v; want %v", got, want)
	}
	var record map[string]any
	for _, line := range strings.Split(strings.TrimSpace(records.String()), "\n") {
		var candidate map[string]any
		if json.Unmarshal([]byte(line), &candidate) == nil && candidate["msg"] == "internal.grpc.request" && candidate["grpc.method"] == sessionStreamMethod {
			record = candidate
		}
	}
	for key, value := range want {
		if record[key] != value {
			t.Fatalf("server record = %v; want %s %s from the first message", record, key, value)
		}
	}
	got = map[string]string{}
	for _, exported := range spans.GetSpans() {
		if exported.Name != sessionStreamMethod {
			continue
		}
		for _, attribute := range exported.Attributes {
			if _, ok := want[string(attribute.Key)]; ok {
				got[string(attribute.Key)] = attribute.Value.AsString()
			}
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("server span fields = %v; want %v", got, want)
	}
}
