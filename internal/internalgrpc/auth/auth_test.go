package auth

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestLoadConfigRejectsInvalidAudienceAndWildcardAllowlist(t *testing.T) {
	for _, env := range []envMap{
		{"TETRAL_INTERNAL_GRPC_AUDIENCE": "other", "TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS": "runtime/agent"},
		{"TETRAL_INTERNAL_GRPC_AUDIENCE": Audience, "TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS": "*"},
		{"TETRAL_INTERNAL_GRPC_AUDIENCE": Audience, "TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS": ""},
	} {
		if _, err := LoadConfig(env); err == nil {
			t.Fatalf("LoadConfig accepted %#v", env)
		}
	}
}

func TestTokenFromIncomingContextRequiresExactlyOneBearerToken(t *testing.T) {
	for _, test := range []struct {
		name string
		ctx  context.Context
	}{
		{name: "missing", ctx: context.Background()},
		{name: "duplicated", ctx: metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer one", "authorization", "Bearer two"))},
		{name: "non bearer", ctx: metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Basic token"))},
		{name: "empty", ctx: metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "))},
		{name: "malformed", ctx: metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer one two"))},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := TokenFromIncomingContext(test.ctx)
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("TokenFromIncomingContext error = %v; want Unauthenticated", err)
			}
		})
	}
}

func TestTokenReviewAuthenticatorUsesExactAudienceAndVerifiedIdentity(t *testing.T) {
	client := &recordingTokenReviewClient{
		response: &authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{
			Authenticated: true,
			Audiences:     []string{Audience},
			User: authenticationv1.UserInfo{
				Username: "system:serviceaccount:runtime:agent",
				Extra: map[string]authenticationv1.ExtraValue{
					"authentication.kubernetes.io/pod-uid": {"pod-uid-tokenreview"},
				},
			},
		}},
	}
	cfg := Config{
		Audience:               Audience,
		AllowedServiceAccounts: []ServiceAccount{{Namespace: "runtime", Name: "agent"}},
	}
	identity, err := NewTokenReviewAuthenticator(client, cfg).Authenticate(context.Background(), "secret-token")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if identity.ServiceAccount.String() != "runtime/agent" {
		t.Fatalf("identity = %s; want runtime/agent", identity.ServiceAccount.String())
	}
	if identity.KubernetesPodUID != "pod-uid-tokenreview" {
		t.Fatalf("identity pod UID = %q; want verified TokenReview pod UID", identity.KubernetesPodUID)
	}
	if client.request.Spec.Token != "secret-token" {
		t.Fatal("TokenReview did not send bearer token")
	}
	if len(client.request.Spec.Audiences) != 1 || client.request.Spec.Audiences[0] != Audience {
		t.Fatalf("TokenReview audiences = %#v; want exact %s", client.request.Spec.Audiences, Audience)
	}
}

func TestTokenReviewAuthenticatorRejectsInvalidResponses(t *testing.T) {
	cfg := Config{
		Audience:               Audience,
		AllowedServiceAccounts: []ServiceAccount{{Namespace: "runtime", Name: "agent"}},
	}
	for _, test := range []struct {
		name     string
		response *authenticationv1.TokenReview
		err      error
		wantCode codes.Code
	}{
		{name: "review error", err: errors.New("provider body bearer-token raw payload"), wantCode: codes.Unauthenticated},
		{name: "not authenticated", response: tokenReviewStatus(false, []string{Audience}, "system:serviceaccount:runtime:agent"), wantCode: codes.Unauthenticated},
		{name: "missing audience", response: tokenReviewStatus(true, nil, "system:serviceaccount:runtime:agent"), wantCode: codes.Unauthenticated},
		{name: "empty audience", response: tokenReviewStatus(true, []string{}, "system:serviceaccount:runtime:agent"), wantCode: codes.Unauthenticated},
		{name: "wrong audience", response: tokenReviewStatus(true, []string{"other"}, "system:serviceaccount:runtime:agent"), wantCode: codes.Unauthenticated},
		{name: "no identity", response: tokenReviewStatus(true, []string{Audience}, ""), wantCode: codes.Unauthenticated},
		{name: "unapproved identity", response: tokenReviewStatus(true, []string{Audience}, "system:serviceaccount:runtime:other"), wantCode: codes.PermissionDenied},
		{name: "wrong namespace same name", response: tokenReviewStatus(true, []string{Audience}, "system:serviceaccount:other:agent"), wantCode: codes.PermissionDenied},
		{name: "malformed service account username", response: tokenReviewStatus(true, []string{Audience}, "runtime/agent"), wantCode: codes.Unauthenticated},
		{name: "malformed service account namespace only", response: tokenReviewStatus(true, []string{Audience}, "system:serviceaccount:runtime"), wantCode: codes.Unauthenticated},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &recordingTokenReviewClient{response: test.response, err: test.err}
			_, err := NewTokenReviewAuthenticator(client, cfg).Authenticate(context.Background(), "token")
			if status.Code(err) != test.wantCode {
				t.Fatalf("Authenticate error = %v; want %s", err, test.wantCode)
			}
			assertSafeAuthError(t, err)
			// A review that could not be made gives its error to the record.
			var recorded interface{ RecordedCause() error }
			if test.err != nil && (!errors.As(err, &recorded) || !errors.Is(recorded.RecordedCause(), test.err)) {
				t.Fatalf("Authenticate error = %v; want the review's error as its recorded cause", err)
			}
		})
	}
}

func TestTokenReviewAuthenticatorIgnoresCallerSuppliedIdentityMetadata(t *testing.T) {
	client := &recordingTokenReviewClient{
		response: tokenReviewStatusWithPodUID(true, []string{Audience}, "system:serviceaccount:runtime:agent", "pod-uid-from-tokenreview"),
	}
	cfg := Config{
		Audience:               Audience,
		AllowedServiceAccounts: []ServiceAccount{{Namespace: "runtime", Name: "agent"}},
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"caller.service_account", "runtime/spoofed",
		"authentication.kubernetes.io/pod-uid", "pod-uid-spoofed",
	))
	identity, err := NewTokenReviewAuthenticator(client, cfg).Authenticate(ctx, "token")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if identity.ServiceAccount.String() != "runtime/agent" {
		t.Fatalf("identity = %s; want verified TokenReview identity runtime/agent", identity.ServiceAccount.String())
	}
	if identity.KubernetesPodUID != "pod-uid-from-tokenreview" {
		t.Fatalf("pod UID = %q; want TokenReview pod UID", identity.KubernetesPodUID)
	}
}

func TestTokenReviewAuthenticatorReusesCompletedAnswersForTenSeconds(t *testing.T) {
	cfg := Config{
		Audience:               Audience,
		AllowedServiceAccounts: []ServiceAccount{{Namespace: "runtime", Name: "agent"}},
	}
	accepted := tokenReviewStatus(true, []string{Audience}, "system:serviceaccount:runtime:agent")
	rejected := tokenReviewStatus(false, []string{Audience}, "system:serviceaccount:runtime:agent")
	// An evaluation error comes in a successful response, alone or beside
	// otherwise accepting fields.
	evaluationError := tokenReviewStatus(false, nil, "")
	evaluationError.Status.Error = "webhook bearer-token raw payload: connection refused"
	acceptedWithError := accepted.DeepCopy()
	acceptedWithError.Status.Error = evaluationError.Status.Error
	// Each call comes its wait after the one before it and answers code;
	// reviews is the count of reviews after the call.
	type call struct {
		wait    time.Duration
		token   string
		reviews int
		code    codes.Code
	}
	var fill []call
	for i := range maxAnswers + 1 {
		fill = append(fill, call{token: fmt.Sprint("token-", i), reviews: i + 1})
	}
	for _, test := range []struct {
		name string
		// responses are the reviews' answers in turn, the last repeated.
		responses []*authenticationv1.TokenReview
		err       error
		calls     []call
	}{
		{name: "an accepted answer is reused within 10 s", responses: []*authenticationv1.TokenReview{accepted}, calls: []call{
			{token: "a", reviews: 1}, {wait: answerTTL - time.Nanosecond, token: "a", reviews: 1}, {token: "b", reviews: 2},
		}},
		{name: "an answer is not reused after 10 s", responses: []*authenticationv1.TokenReview{accepted}, calls: []call{
			{token: "a", reviews: 1}, {wait: answerTTL, token: "a", reviews: 2},
		}},
		{name: "a rejected answer is reused", responses: []*authenticationv1.TokenReview{rejected}, calls: []call{
			{token: "a", reviews: 1, code: codes.Unauthenticated}, {wait: answerTTL - time.Nanosecond, token: "a", reviews: 1, code: codes.Unauthenticated},
		}},
		{name: "a failed review is not reused", err: errors.New("connection refused"), calls: []call{
			{token: "a", reviews: 1, code: codes.Unauthenticated}, {token: "a", reviews: 2, code: codes.Unauthenticated},
		}},
		{name: "an evaluation error is not reused", responses: []*authenticationv1.TokenReview{evaluationError, accepted}, calls: []call{
			{token: "a", reviews: 1, code: codes.Unauthenticated}, {token: "a", reviews: 2},
		}},
		{name: "an evaluation error beside an accepting answer is refused and not reused", responses: []*authenticationv1.TokenReview{acceptedWithError, accepted}, calls: []call{
			{token: "a", reviews: 1, code: codes.Unauthenticated}, {token: "a", reviews: 2}, {token: "a", reviews: 2},
		}},
		{name: "the oldest answer gives way to a new one", responses: []*authenticationv1.TokenReview{accepted}, calls: append(fill,
			call{token: "token-0", reviews: maxAnswers + 2}, call{token: fmt.Sprint("token-", maxAnswers), reviews: maxAnswers + 2},
		)},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &recordingTokenReviewClient{responses: test.responses, err: test.err}
			authenticator := NewTokenReviewAuthenticator(client, cfg)
			now := time.Unix(1_800_000_000, 0)
			authenticator.now = func() time.Time { return now }
			for i, call := range test.calls {
				now = now.Add(call.wait)
				identity, err := authenticator.Authenticate(context.Background(), call.token)
				if status.Code(err) != call.code {
					t.Fatalf("call %d: Authenticate error = %v; want %s", i, err, call.code)
				}
				if err != nil {
					assertSafeAuthError(t, err)
				} else if identity.ServiceAccount.String() != "runtime/agent" {
					t.Fatalf("call %d: identity = %s; want runtime/agent", i, identity.ServiceAccount.String())
				}
				if client.reviews != call.reviews {
					t.Fatalf("call %d: %d reviews; want %d", i, client.reviews, call.reviews)
				}
			}
		})
	}
}

func TestTokenReviewAuthenticatorKeepsNoTokenInItsAnswers(t *testing.T) {
	const token = "eyJhbGciOiJSUzI1NiJ9.secret-workload-token"
	client := &recordingTokenReviewClient{response: tokenReviewStatus(true, []string{Audience}, "system:serviceaccount:runtime:agent")}
	authenticator := NewTokenReviewAuthenticator(client, Config{
		Audience:               Audience,
		AllowedServiceAccounts: []ServiceAccount{{Namespace: "runtime", Name: "agent"}},
	})
	if _, err := authenticator.Authenticate(context.Background(), token); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if len(authenticator.answers.entries) != 1 {
		t.Fatalf("%d answers; want 1", len(authenticator.answers.entries))
	}
	for key := range authenticator.answers.entries {
		if bytes.Contains(key[:], []byte(token)) || key == sha256.Sum256([]byte(token)) {
			t.Fatalf("answer key %x holds or plainly hashes the token", key)
		}
	}
}

// TestTokenReviewClientAllowsReviewBursts builds the TokenReview client
// from the environment with a limit of 200 requests per second and bursts
// of 400, not client-go's 5 and 10, past which every review waits.
func TestTokenReviewClientAllowsReviewBursts(t *testing.T) {
	server := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(server.Close)
	dir := t.TempDir()
	ca, token := filepath.Join(dir, "ca.crt"), filepath.Join(dir, "token")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(token, []byte("reviewer-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	client, err := NewTokenReviewClientFromEnv(envMap{
		EnvKubernetesAPIServerURL:                 server.URL,
		EnvKubernetesAPICACertPath:                ca,
		EnvKubernetesTokenReviewReviewerTokenPath: token,
	})
	if err != nil {
		t.Fatal(err)
	}
	limiter := client.client.AuthenticationV1().RESTClient().GetRateLimiter()
	admitted := 0
	for admitted < 400 && limiter.TryAccept() {
		admitted++
	}
	if limiter.QPS() != 200 || admitted != 400 {
		t.Fatalf("limit %v per second admitted %d at once; want 200 and 400", limiter.QPS(), admitted)
	}
}

func TestStaticBearerAuthenticatorUsesConfiguredTokenAndSingleAllowedIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("sandbox-secret-token\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	cfg := Config{
		Audience:               Audience,
		AllowedServiceAccounts: []ServiceAccount{{Namespace: "tetral-system", Name: "bridge"}},
	}
	authenticator, err := NewStaticBearerAuthenticatorFromFile(path, cfg)
	if err != nil {
		t.Fatalf("NewStaticBearerAuthenticatorFromFile: %v", err)
	}
	identity, err := authenticator.Authenticate(context.Background(), "sandbox-secret-token")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if identity.ServiceAccount.String() != "tetral-system/bridge" {
		t.Fatalf("identity = %s; want bridge service account", identity.ServiceAccount.String())
	}
	_, err = authenticator.Authenticate(context.Background(), "wrong-token")
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("wrong token error = %v; want unauthenticated", err)
	}
	assertSafeAuthError(t, err)
}

func TestStaticBearerAuthenticatorRejectsUnsafeConfigWithoutLeakingToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	const rawToken = "sandbox-secret-token with-space"
	if err := os.WriteFile(path, []byte(rawToken), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	_, err := NewStaticBearerAuthenticatorFromFile(path, Config{
		Audience:               Audience,
		AllowedServiceAccounts: []ServiceAccount{{Namespace: "tetral-system", Name: "bridge"}},
	})
	if err == nil {
		t.Fatal("NewStaticBearerAuthenticatorFromFile accepted malformed token")
	}
	if strings.Contains(err.Error(), "sandbox-secret-token") || strings.Contains(err.Error(), rawToken) {
		t.Fatalf("error leaks token text: %v", err)
	}
	_, err = NewStaticBearerAuthenticatorFromFile(path, Config{
		Audience: Audience,
		AllowedServiceAccounts: []ServiceAccount{
			{Namespace: "tetral-system", Name: "bridge"},
			{Namespace: "tetral-system", Name: "other"},
		},
	})
	if err == nil {
		t.Fatal("NewStaticBearerAuthenticatorFromFile accepted multiple static identities")
	}
}

func assertSafeAuthError(t *testing.T, err error) {
	t.Helper()
	message := status.Convert(err).Message()
	if message != "unauthenticated" && message != "permission denied" {
		t.Fatalf("Authenticate message = %q; want safe auth message", message)
	}
}

func tokenReviewStatus(authenticated bool, audiences []string, username string) *authenticationv1.TokenReview {
	return tokenReviewStatusWithPodUID(authenticated, audiences, username, "")
}

func tokenReviewStatusWithPodUID(authenticated bool, audiences []string, username string, podUID string) *authenticationv1.TokenReview {
	user := authenticationv1.UserInfo{Username: username}
	if podUID != "" {
		user.Extra = map[string]authenticationv1.ExtraValue{
			"authentication.kubernetes.io/pod-uid": {podUID},
		}
	}
	return &authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{
		Authenticated: authenticated,
		Audiences:     audiences,
		User:          user,
	}}
}

// recordingTokenReviewClient answers each review with response, or with
// responses in turn, the last repeated, when they are set.
type recordingTokenReviewClient struct {
	request   *authenticationv1.TokenReview
	response  *authenticationv1.TokenReview
	responses []*authenticationv1.TokenReview
	err       error
	reviews   int
}

func (c *recordingTokenReviewClient) CreateTokenReview(_ context.Context, request *authenticationv1.TokenReview) (*authenticationv1.TokenReview, error) {
	c.reviews++
	c.request = request.DeepCopy()
	if c.err != nil {
		return nil, c.err
	}
	if len(c.responses) > 0 {
		return c.responses[min(c.reviews, len(c.responses))-1].DeepCopy(), nil
	}
	return c.response.DeepCopy(), nil
}

type envMap map[string]string

func (m envMap) Getenv(key string) string { return m[key] }
