package agentruntimebridge

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/sessionrpc"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	providergatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type largeMCPManifestTransportServer struct {
	providergatewayv1.UnimplementedMcpConnectorServiceServer
}

type failingMCPManifestTransportServer struct {
	providergatewayv1.UnimplementedMcpConnectorServiceServer
	code   codes.Code
	values []string
}

func (s failingMCPManifestTransportServer) ListMcpTools(ctx context.Context, _ *providergatewayv1.ListMcpToolsRequest) (*providergatewayv1.ListMcpToolsResponse, error) {
	if len(s.values) > 0 {
		_ = grpc.SetTrailer(ctx, metadata.MD{mcpFailureKindMetadataKey: s.values})
	}
	return nil, status.Error(s.code, "safe test failure")
}

func (largeMCPManifestTransportServer) ListMcpTools(context.Context, *providergatewayv1.ListMcpToolsRequest) (*providergatewayv1.ListMcpToolsResponse, error) {
	return &providergatewayv1.ListMcpToolsResponse{
		ManifestEtag: "etag_large_transport",
		Tools: []*providergatewayv1.McpToolDefinition{{
			Name:            "large_tool",
			Description:     strings.Repeat("d", 128*1024),
			InputSchemaJson: `{"type":"object"}`,
		}},
	}, nil
}

type largeContextTransportServer struct {
	bridgev1.UnimplementedAgentRuntimeBridgeServiceServer
}

func (largeContextTransportServer) LoadContext(context.Context, *bridgev1.LoadContextRequest) (*bridgev1.LoadContextResponse, error) {
	return &bridgev1.LoadContextResponse{
		ContextJson: strings.Repeat("c", 33*1024*1024),
	}, nil
}

func (largeContextTransportServer) WriteEvent(context.Context, *bridgev1.WriteEventRequest) (*bridgev1.WriteEventResponse, error) {
	return &bridgev1.WriteEventResponse{Outcome: &bridgev1.WriteEventResponse_Committed{Committed: &bridgev1.WriteEventCommitted{}}}, nil
}

func TestGatewayMCPManifestListerReceivesManifestAboveSharedSessionCap(t *testing.T) {
	listener := bufconn.Listen(sessionrpc.MaxMCPConnectorGRPCMessageBytes * 2)
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(sessionrpc.MaxMCPConnectorGRPCMessageBytes),
		grpc.MaxSendMsgSize(sessionrpc.MaxMCPConnectorGRPCMessageBytes),
	)
	providergatewayv1.RegisterMcpConnectorServiceServer(server, largeMCPManifestTransportServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	lister := NewGatewayMCPManifestLister(
		"passthrough:///mcp-manifest-capacity",
		&countingRuntimeCommandTokenSource{},
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	result, err := lister.ListMCPTools(context.Background(), MCPManifestListRequest{WorkspaceID: "default", SessionID: "sesn_capacity", MCPServerName: "github"})
	if err != nil {
		t.Fatalf("ListMCPTools above shared session cap: %v", err)
	}
	if len(result.Tools) != 1 || len(result.Tools[0].Description) != 128*1024 {
		t.Fatalf("manifest result = %+v; want one 128 KiB description", result)
	}
}

func TestGatewayMCPManifestListerRequiresExactFailureMetadataPair(t *testing.T) {
	tests := []struct {
		name       string
		code       codes.Code
		values     []string
		diagnostic string
	}{
		{name: "credential unavailable", code: codes.FailedPrecondition, values: []string{"credential_unavailable"}, diagnostic: mcpManifestDiagnosticCredentialUnavailable},
		{name: "server unavailable", code: codes.Unavailable, values: []string{"server_unavailable"}, diagnostic: mcpManifestDiagnosticDiscoveryUnavailable},
		{name: "discovery timeout", code: codes.DeadlineExceeded, values: []string{"discovery_timeout"}, diagnostic: mcpManifestDiagnosticDiscoveryUnavailable},
		{name: "manifest invalid", code: codes.FailedPrecondition, values: []string{"manifest_invalid"}, diagnostic: mcpManifestDiagnosticInvalid},
		{name: "untyped unavailable transport", code: codes.Unavailable, diagnostic: mcpManifestDiagnosticDiscoveryUnavailable},
		{name: "untyped deadline transport", code: codes.DeadlineExceeded, diagnostic: mcpManifestDiagnosticDiscoveryUnavailable},
		{name: "untyped failed precondition", code: codes.FailedPrecondition},
		{name: "duplicate token", code: codes.Unavailable, values: []string{"server_unavailable", "server_unavailable"}},
		{name: "known token with wrong status", code: codes.FailedPrecondition, values: []string{"server_unavailable"}},
		{name: "unknown token", code: codes.Unavailable, values: []string{"other"}},
		{name: "untyped internal", code: codes.Internal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			listener := bufconn.Listen(1024 * 1024)
			server := grpc.NewServer()
			providergatewayv1.RegisterMcpConnectorServiceServer(server, failingMCPManifestTransportServer{code: test.code, values: test.values})
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(server.Stop)

			lister := NewGatewayMCPManifestLister(
				"passthrough:///mcp-manifest-failure",
				&countingRuntimeCommandTokenSource{},
				grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
			)
			_, err := lister.ListMCPTools(context.Background(), MCPManifestListRequest{WorkspaceID: "default", SessionID: "sesn_failure", MCPServerName: "github"})
			var discoveryError mcpManifestDiscoveryError
			if test.diagnostic != "" {
				if !errors.As(err, &discoveryError) || discoveryError.diagnostic != test.diagnostic {
					t.Fatalf("classified error = %T/%v; want discovery diagnostic %q", err, err, test.diagnostic)
				}
				return
			}
			if errors.As(err, &discoveryError) || status.Code(err) != test.code {
				t.Fatalf("classified error = %T/%v; want original status %s", err, err, test.code)
			}
		})
	}
}

func TestRuntimeDurableContextChannelCarriesMessagesAboveNarrowerFuses(t *testing.T) {
	listener := bufconn.Listen(sessionrpc.MaxBridgeAPIGRPCMessageBytes * 2)
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(sessionrpc.MaxBridgeAPIGRPCMessageBytes),
		grpc.MaxSendMsgSize(sessionrpc.MaxBridgeAPIGRPCMessageBytes),
	)
	bridgev1.RegisterAgentRuntimeBridgeServiceServer(server, largeContextTransportServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	connection, err := grpc.NewClient(
		"passthrough:///completion-context-capacity",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(sessionrpc.MaxBridgeAPIGRPCMessageBytes),
			grpc.MaxCallSendMsgSize(sessionrpc.MaxBridgeAPIGRPCMessageBytes),
		),
	)
	if err != nil {
		t.Fatalf("dial completion context transport: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	response, err := bridgev1.NewAgentRuntimeBridgeServiceClient(connection).LoadContext(
		context.Background(),
		&bridgev1.LoadContextRequest{},
	)
	if err != nil {
		t.Fatalf("LoadContext above command fuse: %v", err)
	}
	if len(response.GetContextJson()) != 33*1024*1024 {
		t.Fatalf("LoadContext bytes = %d; want 33 MiB", len(response.GetContextJson()))
	}
	if _, err := bridgev1.NewAgentRuntimeBridgeServiceClient(connection).WriteEvent(
		context.Background(),
		&bridgev1.WriteEventRequest{PayloadJson: strings.Repeat("e", 5*1024*1024)},
	); err != nil {
		t.Fatalf("WriteEvent above command fuse: %v", err)
	}
}

func TestRuntimeDurableContextChannelFailsWhenEitherEndpointUsesNarrowFuse(t *testing.T) {
	tests := []struct {
		name       string
		serverRecv int
		serverSend int
		clientRecv int
		clientSend int
		invoke     func(context.Context, bridgev1.AgentRuntimeBridgeServiceClient) error
	}{
		{
			name:       "client receive",
			serverRecv: sessionrpc.MaxBridgeAPIGRPCMessageBytes,
			serverSend: sessionrpc.MaxBridgeAPIGRPCMessageBytes,
			clientRecv: 32 * 1024 * 1024,
			clientSend: sessionrpc.MaxBridgeAPIGRPCMessageBytes,
			invoke: func(ctx context.Context, client bridgev1.AgentRuntimeBridgeServiceClient) error {
				_, err := client.LoadContext(ctx, &bridgev1.LoadContextRequest{})
				return err
			},
		},
		{
			name:       "server receive",
			serverRecv: 4 * 1024 * 1024,
			serverSend: sessionrpc.MaxBridgeAPIGRPCMessageBytes,
			clientRecv: sessionrpc.MaxBridgeAPIGRPCMessageBytes,
			clientSend: sessionrpc.MaxBridgeAPIGRPCMessageBytes,
			invoke: func(ctx context.Context, client bridgev1.AgentRuntimeBridgeServiceClient) error {
				_, err := client.WriteEvent(ctx, &bridgev1.WriteEventRequest{PayloadJson: strings.Repeat("e", 5*1024*1024)})
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			listener := bufconn.Listen(sessionrpc.MaxBridgeAPIGRPCMessageBytes * 2)
			server := grpc.NewServer(grpc.MaxRecvMsgSize(test.serverRecv), grpc.MaxSendMsgSize(test.serverSend))
			bridgev1.RegisterAgentRuntimeBridgeServiceServer(server, largeContextTransportServer{})
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(server.Stop)

			connection, err := grpc.NewClient(
				"passthrough:///completion-context-capacity-mutation",
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
				grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(test.clientRecv), grpc.MaxCallSendMsgSize(test.clientSend)),
			)
			if err != nil {
				t.Fatalf("dial narrowed durable context transport: %v", err)
			}
			t.Cleanup(func() { _ = connection.Close() })

			if err := test.invoke(context.Background(), bridgev1.NewAgentRuntimeBridgeServiceClient(connection)); status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("narrowed endpoint error = %v; want ResourceExhausted", err)
			}
		})
	}
}
