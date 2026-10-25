package auth

import (
	"context"
	"errors"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/tetral-ai/tetral/internal/workload"
)

const DefaultServiceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token" //nolint:gosec // path to mounted token file, not a credential value.

type TokenSource interface {
	Token(context.Context) (string, error)
}

type FileTokenSource struct {
	Path string
}

func (s FileTokenSource) Token(context.Context) (string, error) {
	path := s.Path
	if path == "" {
		path = DefaultServiceAccountTokenPath
	}
	body, err := os.ReadFile(path) //nolint:gosec // configured Kubernetes service-account token path.
	if err != nil {
		return "", safeTokenError()
	}
	token := strings.TrimSpace(string(body))
	if token == "" || strings.Contains(token, " ") {
		return "", safeTokenError()
	}
	return token, nil
}

type ServiceAccountTokenCredentials struct {
	source TokenSource
}

func NewServiceAccountTokenCredentials(source TokenSource) ServiceAccountTokenCredentials {
	return ServiceAccountTokenCredentials{source: source}
}

func (c ServiceAccountTokenCredentials) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	if c.source == nil {
		return nil, safeTokenError()
	}
	token, err := c.source.Token(ctx)
	if err != nil {
		return nil, safeTokenError()
	}
	return map[string]string{"authorization": "bearer " + token}, nil
}

func (ServiceAccountTokenCredentials) RequireTransportSecurity() bool {
	return false
}

func safeTokenError() error {
	return errors.New("service account token unavailable")
}

// TraceparentUnaryClientInterceptor and TraceparentStreamClientInterceptor
// write the caller's traceparent into every internal call's metadata, so the
// server continues the caller's trace. Every internal dial installs them
// beside the service-account credentials.
func TraceparentUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(withOutgoingTraceparent(ctx), method, req, reply, cc, opts...)
	}
}

func TraceparentStreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(withOutgoingTraceparent(ctx), desc, cc, method, opts...)
	}
}

func withOutgoingTraceparent(ctx context.Context) context.Context {
	if traceparent := workload.ContextTraceparent(ctx); traceparent != "" {
		return metadata.AppendToOutgoingContext(ctx, "traceparent", traceparent)
	}
	return ctx
}
