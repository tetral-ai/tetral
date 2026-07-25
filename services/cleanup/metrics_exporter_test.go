package tetralcleanup

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestOpenMetricsHTTPExporterPushesSchedulerSeriesWithoutScopeLabels(t *testing.T) {
	var body string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			t.Fatalf("method = %s; want POST", request.Method)
		}
		if got := request.Header.Get("Content-Type"); got != openMetricsContentType {
			t.Fatalf("Content-Type = %q; want %q", got, openMetricsContentType)
		}
		data, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		body = string(data)
		writer.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	metrics := NewSchedulerMetrics()
	metrics.ObserveClaimDue(2, 25*time.Millisecond)
	samples, err := metrics.Collector()(context.Background())
	if err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	if err := (OpenMetricsHTTPExporter{Endpoint: server.URL}).Export(context.Background(), samples); err != nil {
		t.Fatalf("Export: %v", err)
	}
	for _, want := range []string{
		"tetral_cleanup_claim_due_runs_total 1",
		"tetral_cleanup_jobs_claimed_total 2",
		"tetral_cleanup_claim_due_duration_ms_total 25",
		"# EOF\n",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("export body missing %q:\n%s", want, body)
		}
	}
	for _, forbidden := range []string{"workspace_id", "workspace.id", "session_id", "session.id"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("export body contains scope label %q:\n%s", forbidden, body)
		}
	}
}

func TestConfigFromEnvValidatesMetricsExporter(t *testing.T) {
	cfg, err := ConfigFromEnv(metricsConfigEnv{
		EnvMetricsExportURL:     "http://otel-collector:4318/v1/metrics",
		EnvMetricsExportTimeout: "1500ms",
	})
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	if cfg.MetricsExportURL != "http://otel-collector:4318/v1/metrics" || cfg.MetricsExportTimeout != 1500*time.Millisecond {
		t.Fatalf("metrics config = %#v", cfg)
	}
	for _, env := range []metricsConfigEnv{
		{EnvMetricsExportURL: "file:///tmp/metrics"},
		{EnvMetricsExportURL: "https://user:secret@example.invalid/metrics"},
		{EnvMetricsExportTimeout: "0s"},
	} {
		if _, err := ConfigFromEnv(env); err == nil {
			t.Fatalf("ConfigFromEnv(%v) succeeded; want validation error", env)
		}
	}
}

type metricsConfigEnv map[string]string

func (e metricsConfigEnv) Getenv(key string) string { return e[key] }
