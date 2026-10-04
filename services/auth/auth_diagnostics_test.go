package tetralauth

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/workload"
)

type failingAuthDiagnosticSink struct{ panic bool }

func (s failingAuthDiagnosticSink) Write([]byte) (int, error) {
	if s.panic {
		panic("private sink failure")
	}
	return 0, errors.New("private sink failure")
}

func TestAuthDiagnosticsBoundedFailuresAndSinkIsolation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		writer io.Writer
	}{
		{"bounded", &bytes.Buffer{}}, {"failed sink", failingAuthDiagnosticSink{}}, {"panicking sink", failingAuthDiagnosticSink{panic: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := workload.NewProcessLogger(tc.writer, "auth", "test", "unit", workload.DefaultDiagnosticConfig())
			defer owner.CloseWithBudget()
			cfg := RouterConfig{Logger: owner.Logger, ExchangeLimits: DefaultExchangeLimits()}
			cfg.ExchangeLimits.RequestsPerMinute = 6000
			router := NewRouter(cfg)
			for n := 0; n < 100; n++ {
				request := httptest.NewRequest(http.MethodPost, "/v1/oauth/token?workspace_id=PRIVATE_SELECTOR_SENTINEL", strings.NewReader(`{"assertion":"PRIVATE_ASSERTION_SENTINEL","unexpected":"PRIVATE_BODY_SENTINEL"}`))
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("Authorization", "Bearer PRIVATE_BEARER_SENTINEL")
				request.Header.Set("X-Api-Key", "PRIVATE_KEY_SENTINEL")
				response := httptest.NewRecorder()
				router.ServeHTTP(response, request)
				if response.Code != http.StatusBadRequest {
					t.Fatalf("diagnostic sink changed handler status=%d", response.Code)
				}
			}
			owner.CloseWithBudget()
			stats := owner.Stats()
			if stats.Suppressed != 99 || stats.LimiterEntries > 1 {
				t.Fatalf("fixed auth failure storm not bounded: %+v", stats)
			}
			if output, ok := tc.writer.(*bytes.Buffer); ok {
				if !strings.Contains(output.String(), `"auth.stage":"input"`) || !strings.Contains(output.String(), `"auth.result":"invalid"`) || !strings.Contains(output.String(), `"error.code":"auth_input_invalid"`) {
					t.Fatal("actual process logger stripped auth classification")
				}
				if strings.Contains(output.String(), "PRIVATE_") || strings.Contains(output.String(), "auth.identity.kind") || strings.Contains(output.String(), "auth.rule.revision") {
					t.Fatal("unverified input became diagnostic identity or leaked content")
				}
			} else if stats.SinkFailures == 0 {
				t.Fatal("failed auth diagnostic sink not accounted")
			}
		})
	}
}

func TestAuthDiagnosticsExchangeLimitCategory(t *testing.T) {
	var output bytes.Buffer
	owner := workload.NewProcessLogger(&output, "auth", "test", "unit", workload.DefaultDiagnosticConfig())
	defer owner.CloseWithBudget()
	limits := DefaultExchangeLimits()
	limits.RequestsPerMinute = 1
	server := httptest.NewServer(NewRouter(RouterConfig{Logger: owner.Logger, ExchangeLimits: limits}))
	defer server.Close()
	for _, want := range []int{http.StatusBadRequest, http.StatusTooManyRequests} {
		response, err := server.Client().Post(server.URL+"/v1/oauth/token", "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		if response.StatusCode != want {
			t.Fatalf("status=%d want=%d", response.StatusCode, want)
		}
		if want == http.StatusTooManyRequests && response.Header.Get("Retry-After") != "60" {
			t.Fatal("exchange limit lost retry metadata")
		}
	}
	owner.CloseWithBudget()
	found := false
	for _, line := range bytes.Split(bytes.TrimSpace(output.Bytes()), []byte("\n")) {
		var event map[string]any
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatal(err)
		}
		if event["event"] != "auth.limiter.limited" {
			continue
		}
		found = true
		if event["auth.stage"] != "limiter" || event["auth.result"] != "limited" || event["error.class"] != "rate_limit_error" || event["error.code"] != "auth_limiter_limited" {
			t.Fatal("exchange limit misclassified as policy/store denial")
		}
	}
	if !found {
		t.Fatal("actual exchange limit diagnostic missing")
	}
}
