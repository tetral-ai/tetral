package auth

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

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

type recordingTokenReviewClient struct {
	request  *authenticationv1.TokenReview
	response *authenticationv1.TokenReview
	err      error
}

func (c *recordingTokenReviewClient) CreateTokenReview(_ context.Context, request *authenticationv1.TokenReview) (*authenticationv1.TokenReview, error) {
	c.request = request.DeepCopy()
	if c.err != nil {
		return nil, c.err
	}
	return c.response.DeepCopy(), nil
}

type envMap map[string]string

func (m envMap) Getenv(key string) string { return m[key] }
