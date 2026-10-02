package integration

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/mcpmanifest"
	providergatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"
)

type failingMCPManifestTransportServer struct {
	providergatewayv1.UnimplementedMcpConnectorServiceServer
	code   codes.Code
	values []string
}

func (s failingMCPManifestTransportServer) ListMcpTools(ctx context.Context, _ *providergatewayv1.ListMcpToolsRequest) (*providergatewayv1.ListMcpToolsResponse, error) {
	if len(s.values) > 0 {
		_ = grpc.SetTrailer(ctx, metadata.MD{mcpmanifest.FailureKindMetadataKey: s.values})
	}
	return nil, status.Error(s.code, "safe test failure")
}
