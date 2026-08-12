package agentruntimebridge

import (
	"context"
	"encoding/json"

	internalgrpc "github.com/tetral-ai/tetral/internal/internalgrpc"
	internalgrpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/queue"
	providergatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const mcpFailureKindMetadataKey = "tetral-mcp-failure-kind"

// MaxMcpManifestBytes (256 KiB per server) bounds a server's accepted
// tools_json. Bridge is the AUTHORITATIVE enforcement point: the check runs
// Bridge-side at manifest acceptance (the ListMcpTools response and every
// changed-manifest re-list), so the connector's own pre-check is only advisory.
// An over-cap manifest fails that server's toolset CLOSED as unready rather
// than truncating its tools.
const MaxMcpManifestBytes = queue.MaxMcpManifestBytes

type MCPManifestLister interface {
	ListMCPTools(context.Context, MCPManifestListRequest) (MCPManifestListResult, error)
}

type MCPManifestListRequest struct {
	WorkspaceID   string
	SessionID     string
	MCPServerName string
	ManifestETag  string
}

type MCPManifestListResult struct {
	ManifestETag string
	Tools        []MCPManifestTool
}

type mcpManifestDiscoveryError struct {
	diagnostic string
}

func (e mcpManifestDiscoveryError) Error() string {
	return "mcp manifest discovery failed"
}

type MCPManifestTool struct {
	Name            string
	Description     string
	InputSchemaJSON string
}

type GatewayMCPManifestLister struct {
	Address     string
	TokenSource internalgrpcauth.TokenSource
	DialOptions []grpc.DialOption
}

func NewGatewayMCPManifestLister(address string, tokenSource internalgrpcauth.TokenSource, dialOptions ...grpc.DialOption) *GatewayMCPManifestLister {
	return &GatewayMCPManifestLister{Address: address, TokenSource: tokenSource, DialOptions: append([]grpc.DialOption(nil), dialOptions...)}
}

func (l *GatewayMCPManifestLister) ListMCPTools(ctx context.Context, request MCPManifestListRequest) (MCPManifestListResult, error) {
	if l == nil || l.Address == "" || l.TokenSource == nil {
		return MCPManifestListResult{}, errMCPManifestListerUnavailable()
	}
	options := append([]grpc.DialOption{}, internalgrpc.MCPConnectorRPCDialOptions()...)
	options = append(options, grpc.WithTransportCredentials(insecure.NewCredentials()))
	options = append(options, l.DialOptions...)
	options = append(options, grpc.WithPerRPCCredentials(internalgrpcauth.NewServiceAccountTokenCredentials(l.TokenSource)))
	conn, err := grpc.NewClient(l.Address, options...)
	if err != nil {
		return MCPManifestListResult{}, err
	}
	defer func() { _ = conn.Close() }()
	var trailers metadata.MD
	response, err := providergatewayv1.NewMcpConnectorServiceClient(conn).ListMcpTools(ctx, &providergatewayv1.ListMcpToolsRequest{
		WorkspaceId:   request.WorkspaceID,
		SessionId:     request.SessionID,
		McpServerName: request.MCPServerName,
	}, grpc.Trailer(&trailers))
	if err != nil {
		return MCPManifestListResult{}, classifyMCPManifestListError(err, trailers)
	}
	tools := make([]MCPManifestTool, 0, len(response.GetTools()))
	for _, tool := range response.GetTools() {
		tools = append(tools, MCPManifestTool{
			Name:            tool.GetName(),
			Description:     tool.GetDescription(),
			InputSchemaJSON: tool.GetInputSchemaJson(),
		})
	}
	return MCPManifestListResult{ManifestETag: response.GetManifestEtag(), Tools: tools}, nil
}

func classifyMCPManifestListError(err error, trailers metadata.MD) error {
	code := status.Code(err)
	values := trailers.Get(mcpFailureKindMetadataKey)
	if len(values) == 0 {
		if code == codes.Unavailable || code == codes.DeadlineExceeded {
			return mcpManifestDiscoveryError{diagnostic: mcpManifestDiagnosticDiscoveryUnavailable}
		}
		return err
	}
	if len(values) != 1 {
		return err
	}
	switch {
	case code == codes.FailedPrecondition && values[0] == "credential_unavailable":
		return mcpManifestDiscoveryError{diagnostic: mcpManifestDiagnosticCredentialUnavailable}
	case code == codes.Unavailable && values[0] == "server_unavailable":
		return mcpManifestDiscoveryError{diagnostic: mcpManifestDiagnosticDiscoveryUnavailable}
	case code == codes.DeadlineExceeded && values[0] == "discovery_timeout":
		return mcpManifestDiscoveryError{diagnostic: mcpManifestDiagnosticDiscoveryUnavailable}
	case code == codes.FailedPrecondition && values[0] == "manifest_invalid":
		return mcpManifestDiscoveryError{diagnostic: mcpManifestDiagnosticInvalid}
	default:
		return err
	}
}

func mcpManifestInputSchema(inputSchemaJSON string) (map[string]any, error) {
	var inputSchema map[string]any
	if err := json.Unmarshal([]byte(inputSchemaJSON), &inputSchema); err != nil || inputSchema == nil {
		return nil, errInvalidMCPManifestToolSchema()
	}
	return inputSchema, nil
}

func canonicalMCPManifestToolsJSON(tools []MCPManifestTool) (string, error) {
	type manifestTool struct {
		Name        string         `json:"name"`
		Description string         `json:"description"`
		InputSchema map[string]any `json:"input_schema"`
	}
	canonical := make([]manifestTool, 0, len(tools))
	for _, tool := range tools {
		if tool.Name == "" || tool.InputSchemaJSON == "" {
			return "", status.Error(codes.Internal, "mcp connector returned invalid manifest tool")
		}
		inputSchema, err := mcpManifestInputSchema(tool.InputSchemaJSON)
		if err != nil {
			return "", err
		}
		canonical = append(canonical, manifestTool{Name: tool.Name, Description: tool.Description, InputSchema: inputSchema})
	}
	raw, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func errMCPManifestListerUnavailable() error {
	return status.Error(codes.FailedPrecondition, "mcp manifest lister is unavailable")
}

func errInvalidMCPManifestToolSchema() error {
	return status.Error(codes.Internal, "mcp connector returned invalid manifest tool schema")
}
