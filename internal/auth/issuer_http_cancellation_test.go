package auth

import (
	"bytes"
	"context"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// This test-owned decoder cancels only after the real HTTPS response was read
// and the strict JSON scanner accepted it. It controls the cancellation boundary;
// it does not claim to reconstruct the response bytes from a historical failure.
type cancellationDecodeTarget struct {
	cancel  func()
	wait    context.Context
	entered bool
	body    []byte
}

func (t *cancellationDecodeTarget) UnmarshalJSON(body []byte) error {
	t.entered = true
	t.body = append([]byte(nil), body...)
	if t.cancel != nil {
		t.cancel()
	}
	if t.wait != nil {
		<-t.wait.Done()
	}
	return errors.New("controlled decoder rejection")
}

func cancellationIssuerRule(server *httptest.Server) FederationRule {
	return FederationRule{Issuer: server.URL, AllowedCIDRs: []string{"127.0.0.1/32"}, TrustedCAPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))}
}

func TestIssuerHTTPContextCancellationAtDecode(t *testing.T) {
	const body = `{"keys":[]}`
	var requests atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()
	for _, kind := range []string{"live", "cancelled", "deadline"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			target := &cancellationDecodeTarget{}
			if kind == "cancelled" {
				target.cancel = cancel
			}
			if kind == "deadline" {
				target.wait = ctx
			}
			before := requests.Load()
			err := fetchIssuerJSON(ctx, cancellationIssuerRule(server), server.URL, target)
			if !target.entered || !bytes.Equal(target.body, []byte(body)) || requests.Load() != before+1 {
				t.Fatal("controlled cancellation did not reach real HTTPS decode boundary")
			}
			if kind == "live" {
				var invalid *AuthenticationError
				if !errors.As(err, &invalid) || ctx.Err() != nil {
					t.Fatalf("live invalid response lost fail-closed classification: %T", err)
				}
			} else if !errors.Is(err, ctx.Err()) || ctx.Err() == nil {
				t.Fatalf("completed response masked caller cancellation: %T", err)
			}
		})
	}
}

func TestIssuerHTTPContextCancellationAfterHeaders(t *testing.T) {
	headers := make(chan struct{}, 1)
	exited := make(chan struct{}, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"keys":`))
		w.(http.Flusher).Flush()
		headers <- struct{}{}
		<-r.Context().Done()
		exited <- struct{}{}
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		var target any
		result <- fetchIssuerJSON(ctx, cancellationIssuerRule(server), server.URL, &target)
	}()
	t.Cleanup(func() { cancel(); <-joined })
	select {
	case <-headers:
	case <-time.After(2 * time.Second):
		t.Fatal("actual HTTPS headers did not reach held-body boundary")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("held HTTPS body lost caller cancellation: %T", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled issuer fetch did not join")
	}
	select {
	case <-exited:
	case <-time.After(2 * time.Second):
		t.Fatal("controlled issuer did not observe request cancellation")
	}
}

func TestAssertionVerifierMalformedLiveIssuerResponse(t *testing.T) {
	issuer := newControlledIssuer(t)
	for _, tc := range []struct{ name, body string }{
		{"empty", ""}, {"truncated", `{"keys":`}, {"duplicate", `{"keys":[],"keys":[]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int64
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			verifier := NewAssertionVerifier(ctx)
			defer verifier.Close()
			rule := issuer.rule
			rule.Issuer = server.URL + "/realm"
			rule.DiscoveryURL = ""
			rule.JWKSURL = server.URL + "/keys"
			rule.TrustedCAPEM = cancellationIssuerRule(server).TrustedCAPEM
			assertion := signIssuerAssertion(t, issuer.key, "initial", rule, nil)
			for range 3 {
				proof, err := verifier.Verify(ctx, rule, assertion)
				var invalid *AuthenticationError
				var unavailable *UnavailableError
				if !errors.As(err, &invalid) || errors.As(err, &unavailable) || ctx.Err() != nil || proof != (VerifiedAssertion{}) {
					t.Fatalf("live malformed issuer response did not remain invalid without proof: %T", err)
				}
			}
			if requests.Load() != 1 {
				t.Fatal("live malformed response bypassed bounded failure cooldown")
			}
		})
	}
}
