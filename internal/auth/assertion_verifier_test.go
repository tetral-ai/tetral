package auth

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type controlledIssuer struct {
	server          *httptest.Server
	rule            FederationRule
	key             *rsa.PrivateKey
	mu              sync.Mutex
	keys            map[string]*rsa.PrivateKey
	requests        map[string]int
	discoveryIssuer string
	jwksURL         string
	status          int
	redirect        string
	oversized       bool
	block           <-chan struct{}
	entered         chan struct{}
}

func newControlledIssuer(t *testing.T) *controlledIssuer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	issuer := &controlledIssuer{key: key, keys: map[string]*rsa.PrivateKey{"initial": key}, requests: map[string]int{}}
	issuer.server = httptest.NewUnstartedServer(http.HandlerFunc(issuer.serve))
	issuer.server.Config.ErrorLog = log.New(io.Discard, "", 0)
	issuer.server.StartTLS()
	t.Cleanup(issuer.server.Close)
	issuer.rule = FederationRule{ID: "rule-controlled", OrganizationID: "organization-a", Issuer: issuer.server.URL + "/realm", Audience: "tetral-engine", DiscoveryURL: issuer.server.URL + "/discovery", AllowedCIDRs: []string{"127.0.0.1/32"}, TrustedCAPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: issuer.server.Certificate().Raw})), Algorithm: "RS256", Enabled: true, Revision: 1}
	issuer.discoveryIssuer = issuer.rule.Issuer
	issuer.jwksURL = issuer.server.URL + "/keys"
	return issuer
}
func (i *controlledIssuer) serve(w http.ResponseWriter, r *http.Request) {
	i.mu.Lock()
	i.requests[r.URL.Path]++
	discoveryIssuer, jwksURL, status, redirect, oversized, block, entered := i.discoveryIssuer, i.jwksURL, i.status, i.redirect, i.oversized, i.block, i.entered
	keys := make(map[string]*rsa.PrivateKey, len(i.keys))
	for kid, key := range i.keys {
		keys[kid] = key
	}
	i.mu.Unlock()
	if r.URL.Path == "/discovery" {
		_ = json.NewEncoder(w).Encode(map[string]string{"issuer": discoveryIssuer, "jwks_uri": jwksURL})
		return
	}
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if block != nil {
		select {
		case <-block:
		case <-r.Context().Done():
			return
		}
	}
	if redirect != "" {
		http.Redirect(w, r, redirect, http.StatusFound)
		return
	}
	if status != 0 {
		w.WriteHeader(status)
		return
	}
	if oversized {
		_, _ = w.Write(make([]byte, maxIssuerResponseBytes+1))
		return
	}
	document := map[string]any{"keys": []any{}}
	for kid, key := range keys {
		document["keys"] = append(document["keys"].([]any), map[string]string{"kty": "RSA", "kid": kid, "alg": "RS256", "use": "sig", "n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes())})
	}
	_ = json.NewEncoder(w).Encode(document)
}
func (i *controlledIssuer) count(path string) int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.requests[path]
}
func signIssuerAssertion(t *testing.T, key *rsa.PrivateKey, kid string, rule FederationRule, extra map[string]any) string {
	t.Helper()
	header := map[string]any{"alg": "RS256", "kid": kid, "typ": "JWT"}
	claims := map[string]any{"iss": rule.Issuer, "sub": "immutable-subject", "aud": rule.Audience, "exp": time.Now().Add(30 * time.Minute).Unix()}
	for name, value := range extra {
		if name == "jku" || name == "x5u" || name == "alg" {
			header[name] = value
		} else {
			claims[name] = value
		}
	}
	headerBytes, err := json.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	claimBytes, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	input := base64.RawURLEncoding.EncodeToString(headerBytes) + "." + base64.RawURLEncoding.EncodeToString(claimBytes)
	digest := sha256.Sum256([]byte(input))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return input + "." + base64.RawURLEncoding.EncodeToString(signature)
}
func assertInvalidAssertion(t *testing.T, err error) {
	t.Helper()
	var invalid *AuthenticationError
	if !errors.As(err, &invalid) {
		t.Fatalf("expected invalid assertion, got %T: %v", err, err)
	}
}

func TestAssertionVerifierRegisteredTrustClaimsAndNoUntrustedURLs(t *testing.T) {
	issuer := newControlledIssuer(t)
	verifier := NewAssertionVerifier(context.Background())
	t.Cleanup(verifier.Close)
	assertion := signIssuerAssertion(t, issuer.key, "initial", issuer.rule, nil)
	proof, err := verifier.Verify(context.Background(), issuer.rule, assertion)
	if err != nil {
		t.Fatal(err)
	}
	if proof.RuleRevision() != 1 || proof.RuleID() != issuer.rule.ID || proof.Subject() != "immutable-subject" || proof.Issuer() != issuer.rule.Issuer {
		t.Fatal("verified immutable proof lost snapshot fields")
	}
	initial := issuer.count("/keys")
	other := newControlledIssuer(t)
	for _, claims := range []map[string]any{{"alg": "none"}, {"alg": "HS256"}, {"iss": "https://unregistered.invalid"}, {"aud": "wrong"}, {"exp": time.Now().Add(-time.Minute).Unix()}, {"nbf": time.Now().Add(time.Minute).Unix()}, {"sub": ""}} {
		_, err := verifier.Verify(context.Background(), issuer.rule, signIssuerAssertion(t, issuer.key, "initial", issuer.rule, claims))
		assertInvalidAssertion(t, err)
	}
	_, err = verifier.Verify(context.Background(), issuer.rule, signIssuerAssertion(t, other.key, "initial", issuer.rule, nil))
	assertInvalidAssertion(t, err)
	_, err = verifier.Verify(context.Background(), issuer.rule, signIssuerAssertion(t, issuer.key, "initial", issuer.rule, map[string]any{"jku": other.server.URL + "/keys", "x5u": "http://169.254.169.254/credentials"}))
	if err != nil {
		t.Fatal(err)
	}
	if issuer.count("/keys") != initial || other.count("/keys") != 0 {
		t.Fatal("invalid known claims/signature or JWT URL caused issuer fetch")
	}
	denied := issuer.rule
	denied.AllowedCIDRs = nil
	denied.Revision++
	_, err = verifier.Verify(context.Background(), denied, assertion)
	assertInvalidAssertion(t, err)
	if issuer.count("/keys") != initial {
		t.Fatal("unconfigured loopback received request")
	}
	noCA := issuer.rule
	noCA.TrustedCAPEM = ""
	noCA.Revision += 2
	_, err = verifier.Verify(context.Background(), noCA, assertion)
	var unavailable *UnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("untrusted certificate accepted or misclassified: %T", err)
	}
	if issuer.count("/keys") != initial {
		t.Fatal("TLS-untrusted issuer received HTTP request")
	}
}

func TestAssertionVerifierDiscoveryDenialsAndDependencyFailures(t *testing.T) {
	cases := []struct {
		name        string
		configure   func(*controlledIssuer)
		unavailable bool
	}{
		{"wrong issuer", func(i *controlledIssuer) { i.discoveryIssuer = "https://wrong.invalid" }, false},
		{"HTTP metadata URI", func(i *controlledIssuer) { i.jwksURL = "http://169.254.169.254/latest/meta-data" }, false},
		{"cross origin URI", func(i *controlledIssuer) { i.jwksURL = "https://unregistered.invalid/keys" }, false},
		{"redirect", func(i *controlledIssuer) { i.redirect = "https://unregistered.invalid/keys" }, false},
		{"outage", func(i *controlledIssuer) { i.status = http.StatusServiceUnavailable }, true},
		{"oversized", func(i *controlledIssuer) { i.oversized = true }, true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			issuer := newControlledIssuer(t)
			test.configure(issuer)
			verifier := NewAssertionVerifier(context.Background())
			defer verifier.Close()
			_, err := verifier.Verify(context.Background(), issuer.rule, signIssuerAssertion(t, issuer.key, "initial", issuer.rule, nil))
			if test.unavailable {
				var unavailable *UnavailableError
				if !errors.As(err, &unavailable) {
					t.Fatalf("dependency failure misclassified: %T", err)
				}
			} else {
				assertInvalidAssertion(t, err)
			}
			if (test.name == "wrong issuer" || test.name == "HTTP metadata URI" || test.name == "cross origin URI") && issuer.count("/keys") != 0 {
				t.Fatal("prohibited discovery destination fetched")
			}
		})
	}
}

func TestAssertionVerifierRotationCooldownRecordedValidityAndRevision(t *testing.T) {
	issuer := newControlledIssuer(t)
	verifier := NewAssertionVerifier(context.Background())
	defer verifier.Close()
	clock := time.Now()
	verifier.now = func() time.Time { return clock }
	old := signIssuerAssertion(t, issuer.key, "initial", issuer.rule, nil)
	if _, err := verifier.Verify(context.Background(), issuer.rule, old); err != nil {
		t.Fatal(err)
	}
	rotated, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	issuer.mu.Lock()
	issuer.keys["rotated"] = rotated
	issuer.mu.Unlock()
	fresh := signIssuerAssertion(t, rotated, "rotated", issuer.rule, nil)
	if _, err := verifier.Verify(context.Background(), issuer.rule, fresh); err != nil {
		t.Fatal(err)
	}
	if issuer.count("/keys") != 2 {
		t.Fatal("unknown new kid did not trigger exactly one refresh")
	}
	if _, err := verifier.Verify(context.Background(), issuer.rule, old); err != nil {
		t.Fatal("overlap key rejected", err)
	}
	missing := signIssuerAssertion(t, rotated, "missing", issuer.rule, nil)
	for n := 0; n < 5; n++ {
		_, err := verifier.Verify(context.Background(), issuer.rule, missing)
		assertInvalidAssertion(t, err)
	}
	if issuer.count("/keys") != 2 {
		t.Fatal("unknown-kid cooldown bypassed")
	}
	clock = clock.Add(issuerUnknownKidCooldown + time.Second)
	_, err = verifier.Verify(context.Background(), issuer.rule, missing)
	assertInvalidAssertion(t, err)
	if issuer.count("/keys") != 3 {
		t.Fatal("cooldown did not permit one subsequent refresh")
	}
	issuer.mu.Lock()
	issuer.status = http.StatusServiceUnavailable
	issuer.mu.Unlock()
	clock = clock.Add(issuerKeyTTL - time.Minute)
	if _, err := verifier.Verify(context.Background(), issuer.rule, old); err != nil {
		t.Fatal("known key rejected before recorded cache expiry", err)
	}
	_, err = verifier.Verify(context.Background(), issuer.rule, missing)
	var unavailable *UnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("unknown key during outage misclassified: %T", err)
	}
	clock = clock.Add(2 * time.Minute)
	_, err = verifier.Verify(context.Background(), issuer.rule, old)
	if !errors.As(err, &unavailable) {
		t.Fatal("expired keys accepted during outage", err)
	}
	issuer.mu.Lock()
	issuer.status = 0
	delete(issuer.keys, "initial")
	issuer.mu.Unlock()
	rule := issuer.rule
	rule.Revision++
	if _, err := verifier.Verify(context.Background(), rule, fresh); err != nil {
		t.Fatal(err)
	}
	_, err = verifier.Verify(context.Background(), rule, old)
	assertInvalidAssertion(t, err)
	_, err = verifier.Verify(context.Background(), issuer.rule, old)
	assertInvalidAssertion(t, err)
}

func TestAssertionVerifierCancellationAndShutdownJoin(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(map[bool]string{false: "caller cancellation", true: "shutdown"}[shutdown], func(t *testing.T) {
			issuer := newControlledIssuer(t)
			issuer.block = make(chan struct{})
			issuer.entered = make(chan struct{}, 1)
			verifier := NewAssertionVerifier(context.Background())
			defer verifier.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := verifier.Verify(ctx, issuer.rule, signIssuerAssertion(t, issuer.key, "initial", issuer.rule, nil))
				result <- err
			}()
			select {
			case <-issuer.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("HTTP refresh never entered")
			}
			if shutdown {
				done := make(chan struct{})
				go func() { verifier.Close(); close(done) }()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("shutdown did not cancel/join refresh")
				}
			} else {
				cancel()
			}
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("canceled refresh produced proof")
				}
			case <-time.After(time.Second):
				t.Fatal("canceled verifier did not return")
			}
		})
	}
}
