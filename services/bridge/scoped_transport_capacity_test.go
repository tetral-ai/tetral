package agentruntimebridge

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/sessionrpc"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	providergatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type largeMCPManifestTransportServer struct {
	providergatewayv1.UnimplementedMcpConnectorServiceServer
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

type largeTaskResultTransportServer struct {
	bridgev1.UnimplementedAgentRuntimeBridgeServiceServer
}

type largeContextTransportServer struct {
	bridgev1.UnimplementedAgentRuntimeBridgeServiceServer
}

func (largeContextTransportServer) LoadContext(context.Context, *bridgev1.LoadContextRequest) (*bridgev1.LoadContextResponse, error) {
	return &bridgev1.LoadContextResponse{
		Ack:         committedAck("", ""),
		ContextJson: strings.Repeat("c", 5*1024*1024),
	}, nil
}

func (largeTaskResultTransportServer) ReadCommandResult(context.Context, *bridgev1.ReadCommandResultRequest) (*bridgev1.ReadCommandResultResponse, error) {
	return &bridgev1.ReadCommandResultResponse{ResultJson: strings.Repeat("r", 2*1024*1024)}, nil
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
		sandboxReleaseClientTestTokenSource{},
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

func TestTaskNotificationReaderReceivesStoredResultAboveSharedSessionCap(t *testing.T) {
	listener := bufconn.Listen(sessionrpc.MaxTaskResultGRPCMessageBytes * 2)
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(sessionrpc.MaxTaskResultGRPCMessageBytes),
		grpc.MaxSendMsgSize(sessionrpc.MaxTaskResultGRPCMessageBytes),
	)
	bridgev1.RegisterAgentRuntimeBridgeServiceServer(server, largeTaskResultTransportServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	reader, connection, err := DialBridgeAPITaskNotificationReader(
		"passthrough:///task-result-capacity",
		sandboxReleaseClientTestTokenSource{},
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	if err != nil {
		t.Fatalf("dial task result reader: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	result, err := reader.ReadTaskNotificationResult(context.Background(), &bridgev1.RuntimeScope{}, "task_capacity", "sevt_capacity")
	if err != nil {
		t.Fatalf("ReadTaskNotificationResult above shared session cap: %v", err)
	}
	if len(result) != 2*1024*1024 {
		t.Fatalf("task result bytes = %d; want 2 MiB", len(result))
	}
}

func TestRuntimeContextChannelCarriesCompletionWindowAboveCommandFuse(t *testing.T) {
	listener := bufconn.Listen(sessionrpc.MaxAttachmentGRPCMessageBytes * 2)
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(sessionrpc.MaxAttachmentGRPCMessageBytes),
		grpc.MaxSendMsgSize(sessionrpc.MaxAttachmentGRPCMessageBytes),
	)
	bridgev1.RegisterAgentRuntimeBridgeServiceServer(server, largeContextTransportServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	connection, err := grpc.NewClient(
		"passthrough:///completion-context-capacity",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(sessionrpc.MaxAttachmentGRPCMessageBytes),
			grpc.MaxCallSendMsgSize(sessionrpc.MaxAttachmentGRPCMessageBytes),
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
	if len(response.GetContextJson()) != 5*1024*1024 {
		t.Fatalf("LoadContext bytes = %d; want 5 MiB", len(response.GetContextJson()))
	}
}
