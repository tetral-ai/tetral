package auth

import (
	"context"
	"errors"
	"os"
	"strings"
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
