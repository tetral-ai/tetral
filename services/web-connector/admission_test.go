package webconnector

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/internalgrpc"
	grpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	providergatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"
)

func TestBindingAdmissionRejectsEveryTamperedClaimBeforeBlobOrBackendAccess(t *testing.T) {
	t.Parallel()
	nowValue := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return nowValue }
	key := []byte("binding-verifier-key-with-at-least-32-bytes")
	tests := []struct {
		name         string
		mutate       func(*providergatewayv1.RunWebRequest)
		podUID       string
		replaceToken func(*providergatewayv1.RunWebRequest) string
	}{
		{name: "workspace claim", mutate: func(request *providergatewayv1.RunWebRequest) { request.WorkspaceId = "other-workspace" }},
		{name: "session claim", mutate: func(request *providergatewayv1.RunWebRequest) { request.SessionId = "other-session" }},
		{name: "thread claim", mutate: func(request *providergatewayv1.RunWebRequest) { request.SessionThreadId = "other-thread" }},
		{name: "binding claim", mutate: func(request *providergatewayv1.RunWebRequest) { request.BindingId = "other-binding" }},
		{name: "binding generation claim", mutate: func(request *providergatewayv1.RunWebRequest) { request.BindingGeneration++ }},
		{name: "runtime pod claim", podUID: "other-runtime-pod"},
		{name: "expiration claim", replaceToken: func(request *providergatewayv1.RunWebRequest) string {
			return signRequest(request, "runtime-pod", nowValue, key)
		}},
		{name: "malformed payload", replaceToken: func(*providergatewayv1.RunWebRequest) string {
			return signedBindingPayload([]byte("{"), key)
		}},
		{name: "malformed signature", replaceToken: func(request *providergatewayv1.RunWebRequest) string {
			parts := strings.Split(request.GetRuntimeBindingToken(), ".")
			return parts[0] + "." + parts[1] + ".malformed"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := &fakeBackend{}
			service := NewService(panicBlobStore{}, backend, NewBindingVerifier(key, now), NewMetrics(), now, nil)
			request := bindingAdmissionRequest()
			request.RuntimeBindingToken = signRequest(request, "runtime-pod", nowValue.Add(time.Hour), key)
			if test.mutate != nil {
				test.mutate(request)
			}
			if test.replaceToken != nil {
				request.RuntimeBindingToken = test.replaceToken(request)
			}
			podUID := test.podUID
			if podUID == "" {
				podUID = "runtime-pod"
			}
			_, err := service.RunWeb(bindingAdmissionContext(podUID), request)
			if status.Code(err) != codes.PermissionDenied {
				t.Fatalf("code=%s err=%v", status.Code(err), err)
			}
			if backend.calls != 0 {
				t.Fatalf("backend calls=%d", backend.calls)
			}
		})
	}
}

func TestBindingAdmissionAcceptsMatchingClaimsBeforeHarmlessExecution(t *testing.T) {
	t.Parallel()
	nowValue := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return nowValue }
	key := []byte("binding-verifier-key-with-at-least-32-bytes")
	objects := blob.NewFakeBlobStore()
	backend := &fakeBackend{}
	service := NewService(objects, backend, NewBindingVerifier(key, now), NewMetrics(), now, nil)
	request := bindingAdmissionRequest()
	request.RuntimeBindingToken = signRequest(request, "runtime-pod", nowValue.Add(time.Hour), key)
	response, err := service.RunWeb(bindingAdmissionContext("runtime-pod"), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED {
		t.Fatalf("status=%s", response.GetStatus())
	}
	if backend.calls != 1 || objects.Len() != 1 {
		t.Fatalf("backend calls=%d blob objects=%d; want harmless execution side effects", backend.calls, objects.Len())
	}
}

func TestWebPortLeavesSiblingProviderStreamUnimplemented(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	backend := &fakeBackend{}
	service, key, now := testService(blob.NewFakeBlobStore(), backend)
	server, err := internalgrpc.NewServer(internalgrpc.Config{
		ServiceName:      ServiceName,
		Listener:         listener,
		Authenticator:    fixedAuthenticator{identity: grpcauth.Identity{ServiceAccount: grpcauth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"}, KubernetesPodUID: "runtime-pod"}},
		MethodAuthorizer: MethodAuthorizer,
		Register:         func(server *grpc.Server) { Register(server, service) },
		ServerOptions: []grpc.ServerOption{
			grpc.MaxRecvMsgSize(maxRunWebRequestGRPCMessageBytes),
			grpc.MaxSendMsgSize(maxRunWebResponseGRPCMessageBytes),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	connection, err := grpc.NewClient("passthrough:///buffer", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer workload-token")
	client := providergatewayv1.NewProviderGatewayServiceClient(connection)
	queries := make([]*providergatewayv1.WebSearchQuery, maxOperations)
	for index := range queries {
		marker := fmt.Sprintf("QUERY_%d_", index)
		queries[index] = &providergatewayv1.WebSearchQuery{Q: marker + strings.Repeat("q", maxRequestTextBytes-len(marker))}
	}
	response, err := client.RunWeb(ctx, testRequest(&providergatewayv1.WebToolInput{SearchQuery: queries}, "event-grpc-eight", key, now))
	if err != nil {
		t.Fatalf("RunWeb: %v", err)
	}
	if response.GetStatus() != providergatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED || backend.calls != maxOperations {
		t.Fatalf("RunWeb response/backend calls = %+v/%d; want completed/%d", response, backend.calls, maxOperations)
	}
	for index := range queries {
		if strings.Contains(response.GetResultText(), fmt.Sprintf("QUERY_%d_", index)) {
			t.Fatalf("RunWeb response echoed maximum search query %d", index)
		}
	}
	stream, err := client.StreamProviderRequest(ctx, &providergatewayv1.ProviderRequest{})
	if err == nil {
		_, err = stream.Recv()
	}
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("code=%s err=%v", status.Code(err), err)
	}
}

type fixedAuthenticator struct{ identity grpcauth.Identity }

func (a fixedAuthenticator) Authenticate(context.Context, string) (grpcauth.Identity, error) {
	return a.identity, nil
}

func bindingAdmissionRequest() *providergatewayv1.RunWebRequest {
	return &providergatewayv1.RunWebRequest{
		WorkspaceId:       "ws",
		SessionId:         "ses",
		SessionThreadId:   "thr",
		ToolUseEventId:    "evt-admission",
		BindingId:         "bind",
		BindingGeneration: 1,
		Input: &providergatewayv1.WebToolInput{
			SearchQuery: []*providergatewayv1.WebSearchQuery{{Q: "example"}},
		},
	}
}

func bindingAdmissionContext(podUID string) context.Context {
	return grpcauth.ContextWithIdentity(context.Background(), grpcauth.Identity{
		ServiceAccount:   grpcauth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"},
		KubernetesPodUID: podUID,
	})
}

func signedBindingPayload(raw, key []byte) string {
	part := base64.RawURLEncoding.EncodeToString(raw)
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(part))
	return "rtbt_v1." + part + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

type panicBlobStore struct{}

func (panicBlobStore) Put(context.Context, string, io.Reader, int64) error {
	panic("blob access before admission")
}
func (panicBlobStore) Get(context.Context, string) (io.ReadCloser, error) {
	panic("blob access before admission")
}
func (panicBlobStore) HeadObject(context.Context, string) (blob.ObjectMetadata, error) {
	panic("blob access before admission")
}
func (panicBlobStore) CopyObject(context.Context, string, string) error {
	panic("blob access before admission")
}
func (panicBlobStore) Delete(context.Context, string) error { panic("blob access before admission") }
func (panicBlobStore) DeletePrefix(context.Context, string) error {
	panic("blob access before admission")
}
