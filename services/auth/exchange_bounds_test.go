package tetralauth

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
)

type exchangeTestEnv map[string]string

func (e exchangeTestEnv) Getenv(name string) string { return e[name] }

func TestAuthExchangeConfigDefaultAndBounds(t *testing.T) {
	private, err := auth.GenerateEd25519PrivateKeyBase64()
	if err != nil {
		t.Fatal(err)
	}
	base := exchangeTestEnv{EnvBootstrapWorkspaceID: "default", EnvBootstrapAPIKey: strings.Repeat("x", 32), EnvInternalPrincipalPrivateKeyB64: private}
	cfg, err := ConfigFromEnv(base)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.JWKSCacheTTL != 600*time.Second || cfg.ExchangeLimits.BodyBytes != 32768 || cfg.ExchangeLimits.ConcurrentRequests != 32 || cfg.ExchangeLimits.RequestsPerMinute != 60 || cfg.ExchangeLimits.BodyReadTimeout != 5*time.Second {
		t.Fatal("public Auth exchange defaults changed")
	}
	for _, tc := range []struct {
		name, minimum, maximum string
		invalid                []string
	}{
		{EnvJWKSCacheTTLSeconds, "1", "600", []string{"0", "601", "1.5"}},
		{EnvExchangeBodyBytes, "1024", "65536", []string{"1023", "65537", "invalid"}},
		{EnvExchangeConcurrency, "1", "32", []string{"0", "33"}},
		{EnvExchangeRequestsPerMinute, "1", "6000", []string{"0", "6001"}},
		{EnvExchangeBodyReadTimeoutMS, "100", "5000", []string{"99", "5001"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, valid := range []string{tc.minimum, tc.maximum} {
				base[tc.name] = valid
				if _, err := ConfigFromEnv(base); err != nil {
					t.Fatalf("documented bound %s rejected", valid)
				}
			}
			for _, invalid := range tc.invalid {
				base[tc.name] = invalid
				if _, err := ConfigFromEnv(base); err == nil {
					t.Fatal("invalid Auth exchange bound accepted")
				}
			}
			delete(base, tc.name)
		})
	}
}

func TestAuthExchangeStrictJSONAndProtectedResponses(t *testing.T) {
	valid := `{"grant_type":"urn:ietf:params:oauth:grant-type:jwt-bearer","assertion":"opaque-assertion","federation_rule_id":"rule","organization_id":"org"}`
	for _, tc := range []struct {
		name, body, contentType string
		want                    int
	}{
		{"valid unavailable", valid, "application/json", 503},
		{"case equivalent selector", strings.TrimSuffix(valid, "}") + `,"ORGANIZATION_ID":"other"}`, "application/json", 400},
		{"escaped duplicate", strings.TrimSuffix(valid, "}") + `,"organization_\u0069d":"other"}`, "application/json", 400},
		{"unknown", strings.TrimSuffix(valid, "}") + `,"audience":"caller-selected"}`, "application/json", 400},
		{"unsupported grant", strings.Replace(valid, "jwt-bearer", "client-credentials", 1), "application/json", 400},
		{"missing rule", strings.Replace(valid, `,"federation_rule_id":"rule"`, "", 1), "application/json", 400},
		{"array", "[]", "application/json", 400}, {"null", "null", "application/json", 400},
		{"trailing", valid + ` {}`, "application/json", 400},
		{"form", "grant_type=jwt-bearer&assertion=opaque-assertion", "application/x-www-form-urlencoded", 400},
		{"oversize", strings.Repeat(" ", 32769), "application/json", 413},
	} {
		t.Run(tc.name, func(t *testing.T) {
			router := NewRouter(RouterConfig{})
			request := httptest.NewRequest(http.MethodPost, "/v1/oauth/token?workspace_id=ignored", strings.NewReader(tc.body))
			request.Header.Set("Content-Type", tc.contentType)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			if recorder.Code != tc.want {
				t.Fatalf("status=%d want=%d", recorder.Code, tc.want)
			}
			if recorder.Header().Get("Cache-Control") != "no-store" || recorder.Header().Get("Pragma") != "no-cache" {
				t.Fatal("exchange response permits caching")
			}
			if strings.Contains(recorder.Body.String(), "opaque-assertion") || strings.Contains(recorder.Body.String(), "caller-selected") {
				t.Fatal("exchange error disclosed untrusted input")
			}
		})
	}
}

type observedSlowBody struct {
	body    io.ReadCloser
	entered chan struct{}
	once    sync.Once
}

func (b *observedSlowBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.entered) })
	return b.body.Read(p)
}
func (b *observedSlowBody) Close() error {
	b.once.Do(func() { close(b.entered) })
	return b.body.Close()
}

// These use real server sockets. Recorder-only tests cannot exercise deadline,
// body draining or the lifecycle of an acquired concurrency slot.
func TestAuthExchangeSlowBodiesRemainBounded(t *testing.T) {
	for _, contentType := range []string{"application/json", "application/x-www-form-urlencoded"} {
		t.Run(contentType, func(t *testing.T) {
			limits := DefaultExchangeLimits()
			limits.ConcurrentRequests = 1
			limits.BodyReadTimeout = 500 * time.Millisecond
			entered := make(chan struct{})
			router := NewRouter(RouterConfig{ExchangeLimits: limits})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("X-Slow-Fixture") == "yes" {
					r.Body = &observedSlowBody{body: r.Body, entered: entered}
				}
				router.ServeHTTP(w, r)
			}))
			defer server.Close()
			address := strings.TrimPrefix(server.URL, "http://")
			connection, err := net.DialTimeout("tcp", address, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = connection.Close() }()
			if err := connection.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			if _, err := fmt.Fprintf(connection, "POST /v1/oauth/token HTTP/1.1\r\nHost: fixture\r\nContent-Type: %s\r\nContent-Length: 100\r\nX-Slow-Fixture: yes\r\n\r\n{", contentType); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("slow request did not enter actual handler")
			}
			// The socket body has reached Read (valid media) or Close (wrong
			// media), after admission. No scheduling delay substitutes for this.
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			second, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/oauth/token", strings.NewReader("{}"))
			if err != nil {
				t.Fatal(err)
			}
			second.Header.Set("Content-Type", "application/json")
			response, err := server.Client().Do(second)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			cancel()
			if response.StatusCode != http.StatusTooManyRequests {
				t.Fatalf("unfinished first body released its slot: second status=%d", response.StatusCode)
			}
			first, err := http.ReadResponse(bufio.NewReader(connection), &http.Request{Method: http.MethodPost})
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, first.Body)
			_ = first.Body.Close()
			if first.StatusCode != http.StatusBadRequest {
				t.Fatalf("slow body status=%d", first.StatusCode)
			}
			if time.Since(start) > 1500*time.Millisecond {
				t.Fatal("slow body escaped configured read deadline")
			}
			thirdCtx, thirdCancel := context.WithTimeout(context.Background(), time.Second)
			defer thirdCancel()
			third, err := http.NewRequestWithContext(thirdCtx, http.MethodPost, server.URL+"/v1/oauth/token", strings.NewReader("{}"))
			if err != nil {
				t.Fatal(err)
			}
			third.Header.Set("Content-Type", "application/json")
			final, err := server.Client().Do(third)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, final.Body)
			_ = final.Body.Close()
			if final.StatusCode != http.StatusBadRequest {
				t.Fatalf("completed body leaked admission slot: status=%d", final.StatusCode)
			}
		})
	}
}

func TestAuthExchangeRejectedSlowBodyDoesNotDrainForFullBudget(t *testing.T) {
	limits := DefaultExchangeLimits()
	limits.RequestsPerMinute = 1
	server := httptest.NewServer(NewRouter(RouterConfig{ExchangeLimits: limits}))
	defer server.Close()
	// A completed parser rejection consumes the configured per-minute allowance.
	request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/oauth/token", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatal("priming exchange was not admitted")
	}
	connection, err := net.DialTimeout("tcp", strings.TrimPrefix(server.URL, "http://"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	// This deadline is deliberately shorter than the admitted five-second read
	// budget: a rejected body must not start a full-budget drain outside a slot.
	if err := connection.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(connection, "POST /v1/oauth/token HTTP/1.1\r\nHost: fixture\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{"); err != nil {
		t.Fatal(err)
	}
	rejected, err := http.ReadResponse(bufio.NewReader(connection), &http.Request{Method: http.MethodPost})
	if err != nil {
		t.Fatal("admission-rejected body retained a full read/drain budget")
	}
	_, _ = io.Copy(io.Discard, rejected.Body)
	_ = rejected.Body.Close()
	if rejected.StatusCode != http.StatusTooManyRequests || !rejected.Close || rejected.Header.Get("Retry-After") != "60" {
		t.Fatal("admission rejection failed its terminal bounded response contract")
	}
}
