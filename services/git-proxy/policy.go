package gitproxy

import (
	"context"
	"encoding/base64"
	"errors"

	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	DecisionInjected  = "injected"
	DecisionAnonymous = "anonymous"
)

var (
	ErrRepositoryPolicyUnavailable = errors.New("git-proxy repository policy is unavailable")
	ErrGitHubCredentialRequired    = errors.New("github credential is required")
)

type RepositoryAuthRequest struct {
	WorkspaceID workspace.ID
	SessionID   string
	Owner       string
	Repo        string
}

type RepositoryAuthDecision struct {
	Decision      string
	Authorization string
}

type RepositoryAuthorizer interface {
	AuthorizeRepository(context.Context, RepositoryAuthRequest) (RepositoryAuthDecision, error)
}

type RepositoryTokenResolution struct {
	Mounted bool
	Token   string
}

type RepositoryTokenResolver interface {
	ResolveRepositoryToken(context.Context, RepositoryAuthRequest) (RepositoryTokenResolution, error)
}

type RepositoryPolicyAuthorizer struct {
	Repositories RepositoryTokenResolver
}

func NewRepositoryPolicyAuthorizer(repositories RepositoryTokenResolver) RepositoryPolicyAuthorizer {
	return RepositoryPolicyAuthorizer{Repositories: repositories}
}

func (a RepositoryPolicyAuthorizer) AuthorizeRepository(ctx context.Context, request RepositoryAuthRequest) (RepositoryAuthDecision, error) {
	if a.Repositories == nil {
		return RepositoryAuthDecision{}, ErrRepositoryPolicyUnavailable
	}
	resolution, err := a.Repositories.ResolveRepositoryToken(ctx, request)
	if err != nil {
		return RepositoryAuthDecision{}, err
	}
	if !resolution.Mounted {
		return RepositoryAuthDecision{Decision: DecisionAnonymous}, nil
	}
	if resolution.Token == "" {
		return RepositoryAuthDecision{}, ErrGitHubCredentialRequired
	}
	return RepositoryAuthDecision{
		Decision:      DecisionInjected,
		Authorization: GitHubBasicAuthorization(resolution.Token),
	}, nil
}

func GitHubBasicAuthorization(token string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:"+token))
}
