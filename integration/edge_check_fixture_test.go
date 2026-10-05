package integration

import (
	"context"
	"database/sql"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/workspace"
	tetralauth "github.com/tetral-ai/tetral/services/auth"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	authv3 "github.com/envoyproxy/go-control-plane/envoy/service/auth/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// directEdgeCheck is the owning integration migration seam for direct Auth
// oracles. Actual public-edge proofs send these inputs through Envoy instead.
func directEdgeCheck(ctx context.Context, address, method, target, requestID string, headers http.Header) (*authv3.CheckResponse, error) {
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	raw := &corev3.HeaderMap{}
	for name, values := range headers {
		for _, value := range values {
			raw.Headers = append(raw.Headers, &corev3.HeaderValue{Key: name, RawValue: []byte(value)})
		}
	}
	raw.Headers = append(raw.Headers, &corev3.HeaderValue{Key: "x-request-id", RawValue: []byte(requestID)}, &corev3.HeaderValue{Key: "x-forwarded-for", RawValue: []byte("127.0.0.1")})
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return authv3.NewAuthorizationClient(connection).Check(checkCtx, &authv3.CheckRequest{Attributes: &authv3.AttributeContext{Request: &authv3.AttributeContext_Request{Http: &authv3.AttributeContext_HttpRequest{Method: method, Path: target, HeaderMap: raw}}}})
}

func directEdgeCheckStatus(response *authv3.CheckResponse) (int, string) {
	if response == nil {
		return http.StatusServiceUnavailable, ""
	}
	if denied := response.GetDeniedResponse(); denied != nil {
		return int(denied.GetStatus().GetCode()), ""
	}
	if response.GetStatus().GetCode() != 0 || response.GetOkResponse() == nil {
		return http.StatusServiceUnavailable, ""
	}
	principal := ""
	for _, header := range response.GetOkResponse().GetHeaders() {
		if http.CanonicalHeaderKey(header.GetHeader().GetKey()) == "X-Tetral-Internal-Principal" {
			principal = header.GetHeader().GetValue()
		}
	}
	return http.StatusOK, principal
}

// startSDKAuthorization uses the same production Auth domain and Check adapter;
// the Go edge remains a routing fixture, separately from the real Envoy proof.
func startSDKAuthorization(t *testing.T, database *sql.DB, signer *auth.InternalPrincipalSigner, ttl time.Duration) string {
	t.Helper()
	adapter, err := tetralauth.NewExternalAuthorization(tetralauth.ExternalAuthorizationConfig{Authenticator: &auth.RequestAuthenticator{Resolver: auth.NewAuthorityResolver(database, workspace.DefaultID)}, Signer: signer, PrincipalTTL: ttl})
	if err != nil {
		t.Fatal(err)
	}
	return startCheckFixture(t, adapter)
}

type checkFixture struct {
	authv3.UnimplementedAuthorizationServer
	check func(context.Context, *authv3.CheckRequest) (*authv3.CheckResponse, error)
}

func (s checkFixture) Check(ctx context.Context, request *authv3.CheckRequest) (*authv3.CheckResponse, error) {
	return s.check(ctx, request)
}

func startCheckFixture(t *testing.T, adapter authv3.AuthorizationServer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	authv3.RegisterAuthorizationServer(server, adapter)
	joined := make(chan error, 1)
	go func() { joined <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		if err := <-joined; err != nil {
			t.Errorf("join Check fixture: %v", err)
		}
	})
	return listener.Addr().String()
}

func fixtureCheckHeaders(request *authv3.CheckRequest) http.Header {
	headers := http.Header{}
	for _, entry := range request.GetAttributes().GetRequest().GetHttp().GetHeaderMap().GetHeaders() {
		headers.Add(entry.GetKey(), string(entry.GetRawValue()))
	}
	return headers
}
func fixtureCheckAllowed(principal string) *authv3.CheckResponse {
	return &authv3.CheckResponse{Status: &statuspb.Status{}, HttpResponse: &authv3.CheckResponse_OkResponse{OkResponse: &authv3.OkHttpResponse{Headers: []*corev3.HeaderValueOption{{Header: &corev3.HeaderValue{Key: "x-tetral-internal-principal", Value: principal}}}}}}
}
func fixtureCheckDenied(status int) *authv3.CheckResponse {
	return &authv3.CheckResponse{Status: &statuspb.Status{Code: 7}, HttpResponse: &authv3.CheckResponse_DeniedResponse{DeniedResponse: &authv3.DeniedHttpResponse{Status: &typev3.HttpStatus{Code: typev3.StatusCode(status)}}}}
}

func forwardFixtureCheckDenial(w http.ResponseWriter, response *authv3.CheckResponse) {
	denied := response.GetDeniedResponse()
	if denied == nil {
		http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
		return
	}
	for _, header := range denied.GetHeaders() {
		if !strings.HasPrefix(strings.ToLower(header.GetHeader().GetKey()), "x-tetral-") {
			w.Header().Add(header.GetHeader().GetKey(), header.GetHeader().GetValue())
		}
	}
	w.WriteHeader(int(denied.GetStatus().GetCode()))
	_, _ = io.WriteString(w, denied.GetBody())
}
