package agentruntimebridge

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/internalgrpc"
	"github.com/tetral-ai/tetral/internal/sessionrpc"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type attachmentTransportServer struct {
	bridgev1.UnimplementedAgentRuntimeBridgeServiceServer
	data []byte
}

func (s *attachmentTransportServer) ResolveTransientAttachment(context.Context, *bridgev1.ResolveTransientAttachmentRequest) (*bridgev1.ResolveTransientAttachmentResponse, error) {
	return &bridgev1.ResolveTransientAttachmentResponse{
		Outcome: &bridgev1.ResolveTransientAttachmentResponse_Resolved{
			Resolved: &bridgev1.ResolvedTransientAttachment{
				AttachmentRef: "att_transport",
				Mime:          "image/png",
				Filename:      "transport.png",
				Data:          append([]byte(nil), s.data...),
			},
		},
	}, nil
}

func (s *attachmentTransportServer) CommitMcpToolResult(_ context.Context, request *bridgev1.CommitMcpToolResultRequest) (*bridgev1.CommitMcpToolResultResponse, error) {
	s.data = append([]byte(nil), request.GetInlineMedia()[0].GetData()...)
	return &bridgev1.CommitMcpToolResultResponse{RefsOnlyResultJson: request.GetResultJson()}, nil
}

func TestBridgeAttachmentResolveTransportCarriesContractSizedPayloads(t *testing.T) {
	t.Parallel()

	listener := bufconn.Listen(sessionrpc.MaxAttachmentGRPCMessageBytes * 2)
	serverOptions := append(internalgrpc.SessionRPCServerOptions(),
		grpc.MaxRecvMsgSize(sessionrpc.MaxAttachmentGRPCMessageBytes),
		grpc.MaxSendMsgSize(sessionrpc.MaxAttachmentGRPCMessageBytes),
	)
	server := grpc.NewServer(serverOptions...)
	service := &attachmentTransportServer{}
	bridgev1.RegisterAgentRuntimeBridgeServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	connection, err := grpc.NewClient("passthrough:///bridge-attachment-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(sessionrpc.MaxAttachmentGRPCMessageBytes),
			grpc.MaxCallSendMsgSize(sessionrpc.MaxAttachmentGRPCMessageBytes),
		),
	)
	if err != nil {
		t.Fatalf("dial bridge attachment test server: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := bridgev1.NewAgentRuntimeBridgeServiceClient(connection)

	for _, size := range []int{sessionrpc.MaxInboundGRPCMessageBytes + 1, 10 * 1024 * 1024} {
		payload := make([]byte, size)
		payload[0] = 1
		payload[len(payload)-1] = 2
		service.data = payload
		resolved, err := client.ResolveTransientAttachment(context.Background(), &bridgev1.ResolveTransientAttachmentRequest{AttachmentRef: "att_transport"})
		if err != nil {
			t.Fatalf("resolve attachment with %d bytes: %v", size, err)
		}
		if len(resolved.GetResolved().GetData()) != size || resolved.GetResolved().GetData()[0] != 1 || resolved.GetResolved().GetData()[size-1] != 2 {
			t.Fatalf("resolved attachment mismatch for %d bytes", size)
		}
	}
}

func TestBridgeMCPCommitTransportCarriesMaximumBoundedEnvelope(t *testing.T) {
	t.Parallel()

	listener := bufconn.Listen(sessionrpc.MaxAttachmentGRPCMessageBytes * 2)
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(sessionrpc.MaxAttachmentGRPCMessageBytes),
		grpc.MaxSendMsgSize(sessionrpc.MaxAttachmentGRPCMessageBytes),
	)
	service := &attachmentTransportServer{}
	bridgev1.RegisterAgentRuntimeBridgeServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	connection, err := grpc.NewClient("passthrough:///bridge-mcp-commit-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(sessionrpc.MaxAttachmentGRPCMessageBytes),
			grpc.MaxCallSendMsgSize(sessionrpc.MaxAttachmentGRPCMessageBytes),
		),
	)
	if err != nil {
		t.Fatalf("dial bridge MCP commit test server: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := bridgev1.NewAgentRuntimeBridgeServiceClient(connection)
	payload := make([]byte, 10*1024*1024)
	payload[0] = 1
	payload[len(payload)-1] = 2
	maxJSON := `{"value":"` + strings.Repeat("x", 64*1024-12) + `"}`
	request := &bridgev1.CommitMcpToolResultRequest{
		Scope: &bridgev1.RuntimeScope{
			RequestId:       strings.Repeat("r", 128),
			WorkspaceId:     strings.Repeat("w", 128),
			SessionId:       strings.Repeat("s", 128),
			SessionThreadId: strings.Repeat("t", 128),
			Binding: &bridgev1.RuntimeBindingRef{
				BindingId:         strings.Repeat("b", 128),
				BindingGeneration: 1,
				TargetPodUid:      strings.Repeat("p", 128),
			},
		},
		ToolUseEventId:      strings.Repeat("u", 128),
		NormalizedInputHash: strings.Repeat("h", 128),
		McpServerName:       strings.Repeat("m", 128),
		ToolName:            strings.Repeat("n", 128),
		InputJson:           maxJSON,
		ResultJson:          maxJSON,
		InlineMedia: []*bridgev1.McpInlineMedia{{
			Data:              payload,
			Mime:              "application/pdf",
			SuggestedFilename: strings.Repeat("f", 1024),
		}},
	}
	response, err := client.CommitMcpToolResult(context.Background(), request)
	if err != nil {
		t.Fatalf("commit maximum MCP envelope: %v", err)
	}
	if response.GetRefsOnlyResultJson() != maxJSON || len(service.data) != len(payload) || service.data[0] != 1 || service.data[len(service.data)-1] != 2 {
		t.Fatal("maximum MCP envelope did not round-trip byte-identically")
	}
}
