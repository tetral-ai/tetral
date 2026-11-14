package auth

import (
	"context"
	"crypto/subtle"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/workload"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Identity struct {
	ServiceAccount   ServiceAccount
	KubernetesPodUID string
}

type contextKey struct{}

func IdentityFromContext(ctx context.Context) (Identity, bool) {
	identity, ok := ctx.Value(contextKey{}).(Identity)
	return identity, ok
}

func ContextWithIdentity(ctx context.Context, identity Identity) context.Context {
	return context.WithValue(ctx, contextKey{}, identity)
}

type TokenReviewClient interface {
	CreateTokenReview(context.Context, *authenticationv1.TokenReview) (*authenticationv1.TokenReview, error)
}

type ClientsetTokenReviewClient struct {
	client clientset.Interface
}

func NewClientsetTokenReviewClient(client clientset.Interface) *ClientsetTokenReviewClient {
	return &ClientsetTokenReviewClient{client: client}
}

// reviewQPS and reviewBurst replace client-go's 5 QPS and burst of 10, which
// would cap a process at 5 TokenReviews per second; Kubernetes'
// delegating authenticator uses the same values.
const reviewQPS, reviewBurst = 200, 400

func NewInClusterTokenReviewClient() (*ClientsetTokenReviewClient, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, workload.WithoutPaths(err)
	}
	client, err := newReviewClient(config)
	return client, workload.WithoutPaths(err, config.BearerTokenFile, config.CAFile)
}

func NewTokenReviewClientFromEnv(env Env) (*ClientsetTokenReviewClient, error) {
	if env == nil {
		return nil, workload.NewConfigError("environment is required")
	}
	apiServerURL := strings.TrimSpace(env.Getenv(EnvKubernetesAPIServerURL))
	caCertPath := strings.TrimSpace(env.Getenv(EnvKubernetesAPICACertPath))
	reviewerTokenPath := strings.TrimSpace(env.Getenv(EnvKubernetesTokenReviewReviewerTokenPath))
	if apiServerURL == "" {
		return nil, workload.NewConfigError(EnvKubernetesAPIServerURL + " is required")
	}
	if caCertPath == "" {
		return nil, workload.NewConfigError(EnvKubernetesAPICACertPath + " is required")
	}
	if reviewerTokenPath == "" {
		return nil, workload.NewConfigError(EnvKubernetesTokenReviewReviewerTokenPath + " is required")
	}
	config := &rest.Config{
		Host:            strings.TrimRight(apiServerURL, "/"),
		BearerTokenFile: reviewerTokenPath,
		TLSClientConfig: rest.TLSClientConfig{CAFile: caCertPath},
	}
	client, err := newReviewClient(config)
	return client, workload.WithoutPaths(err, reviewerTokenPath, caCertPath)
}

func newReviewClient(config *rest.Config) (*ClientsetTokenReviewClient, error) {
	config.QPS, config.Burst = reviewQPS, reviewBurst
	client, err := clientset.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return NewClientsetTokenReviewClient(client), nil
}

func (c *ClientsetTokenReviewClient) CreateTokenReview(ctx context.Context, review *authenticationv1.TokenReview) (*authenticationv1.TokenReview, error) {
	return c.client.AuthenticationV1().TokenReviews().Create(ctx, review, metav1.CreateOptions{})
}

// TokenReviewAuthenticator authenticates a token by a TokenReview and reuses
// a completed review's answer, accepted or rejected, for answerTTL; a review
// call that fails, or a review whose status reports an evaluation error, is
// refused and not reused.
type TokenReviewAuthenticator struct {
	client   TokenReviewClient
	audience string
	allowed  map[string]struct{}
	answers  *answers
	now      func() time.Time
}

func NewTokenReviewAuthenticator(client TokenReviewClient, cfg Config) *TokenReviewAuthenticator {
	allowed := map[string]struct{}{}
	for _, account := range cfg.AllowedServiceAccounts {
		allowed[account.String()] = struct{}{}
	}
	return &TokenReviewAuthenticator{client: client, audience: cfg.Audience, allowed: allowed, answers: newAnswers(), now: time.Now}
}

type StaticBearerAuthenticator struct {
	token    string
	identity Identity
}

func NewStaticBearerAuthenticatorFromFile(path string, cfg Config) (*StaticBearerAuthenticator, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, workload.NewConfigError("static bearer token path is required")
	}
	if cfg.Audience != Audience {
		return nil, workload.NewConfigError(fmt.Sprintf("TETRAL_INTERNAL_GRPC_AUDIENCE must be %s", Audience))
	}
	if len(cfg.AllowedServiceAccounts) != 1 {
		return nil, workload.NewConfigError("static bearer auth requires exactly one allowed service account")
	}
	body, err := os.ReadFile(path) //nolint:gosec // configured internal bearer token file.
	if err != nil {
		return nil, workload.WithoutPaths(err)
	}
	token := strings.TrimSpace(string(body))
	if token == "" || strings.Contains(token, " ") {
		return nil, workload.NewConfigError("static bearer token file is invalid")
	}
	return &StaticBearerAuthenticator{
		token:    token,
		identity: Identity{ServiceAccount: cfg.AllowedServiceAccounts[0]},
	}, nil
}

func (a *StaticBearerAuthenticator) Authenticate(_ context.Context, token string) (Identity, error) {
	if a == nil || a.token == "" || token == "" || subtle.ConstantTimeCompare([]byte(token), []byte(a.token)) != 1 {
		return Identity{}, unauthenticated()
	}
	return a.identity, nil
}

func (a *TokenReviewAuthenticator) Authenticate(ctx context.Context, token string) (Identity, error) {
	if a == nil || a.client == nil || a.audience != Audience || token == "" {
		return Identity{}, unauthenticated()
	}
	key := a.answers.key(token)
	if cached, ok := a.answers.get(key, a.now()); ok {
		return cached.identity, cached.err
	}
	review := &authenticationv1.TokenReview{
		Spec: authenticationv1.TokenReviewSpec{
			Token:     token,
			Audiences: []string{Audience},
		},
	}
	response, err := a.client.CreateTokenReview(ctx, review)
	if err != nil {
		return Identity{}, &reviewFailedError{cause: err}
	}
	if response == nil || response.Status.Error != "" {
		return Identity{}, unauthenticated()
	}
	identity, err := a.identity(response)
	a.answers.put(key, answer{identity: identity, err: err}, a.now())
	return identity, err
}

// identity is the allowed workload identity that a completed review names.
func (a *TokenReviewAuthenticator) identity(response *authenticationv1.TokenReview) (Identity, error) {
	if !response.Status.Authenticated || !containsAudience(response.Status.Audiences, Audience) {
		return Identity{}, unauthenticated()
	}
	account, err := serviceAccountFromUsername(response.Status.User.Username)
	if err != nil {
		return Identity{}, unauthenticated()
	}
	if _, ok := a.allowed[account.String()]; !ok {
		return Identity{}, permissionDenied()
	}
	return Identity{ServiceAccount: account, KubernetesPodUID: kubernetesPodUID(response.Status.User.Extra)}, nil
}

func TokenFromIncomingContext(ctx context.Context) (string, error) {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) != 1 {
		return "", unauthenticated()
	}
	value := strings.TrimSpace(values[0])
	prefix, token, ok := strings.Cut(value, " ")
	if !ok || !strings.EqualFold(prefix, "bearer") || strings.TrimSpace(token) == "" || strings.Contains(strings.TrimSpace(token), " ") {
		return "", unauthenticated()
	}
	return strings.TrimSpace(token), nil
}

func serviceAccountFromUsername(username string) (ServiceAccount, error) {
	const prefix = "system:serviceaccount:"
	if !strings.HasPrefix(username, prefix) {
		return ServiceAccount{}, fmt.Errorf("not a service account")
	}
	rest := strings.TrimPrefix(username, prefix)
	namespace, name, ok := strings.Cut(rest, ":")
	if !ok || namespace == "" || name == "" || strings.Contains(name, ":") {
		return ServiceAccount{}, fmt.Errorf("not a service account")
	}
	return ServiceAccount{Namespace: namespace, Name: name}, nil
}

func containsAudience(audiences []string, want string) bool {
	for _, audience := range audiences {
		if audience == want {
			return true
		}
	}
	return false
}

func kubernetesPodUID(extra map[string]authenticationv1.ExtraValue) string {
	values := extra["authentication.kubernetes.io/pod-uid"]
	if len(values) == 0 {
		return ""
	}
	return strings.TrimSpace(values[0])
}

func unauthenticated() error {
	return status.Error(codes.Unauthenticated, "unauthenticated")
}

// reviewFailedError answers the caller as unauthenticated and gives the
// failure record the error of the TokenReview that could not be made.
type reviewFailedError struct{ cause error }

func (e *reviewFailedError) Error() string { return unauthenticated().Error() }

func (e *reviewFailedError) GRPCStatus() *status.Status {
	return status.New(codes.Unauthenticated, "unauthenticated")
}

func (e *reviewFailedError) RecordedCause() error { return e.cause }

func permissionDenied() error {
	return status.Error(codes.PermissionDenied, "permission denied")
}
