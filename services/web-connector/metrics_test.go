package webconnector

import (
	"fmt"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestMetricsEndpointExportsOnlyClosedLowCardinalityFamilies(t *testing.T) {
	t.Parallel()
	metrics := NewMetrics()
	metrics.ObserveRequest("open", "completed", 4*time.Millisecond)
	metrics.ObserveRequest("open", "completed", 25*time.Millisecond)
	metrics.ObserveBackend("reader", "success", 765)
	metrics.ObserveRotation("429")
	metrics.ObserveCacheBytes(42)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "http://metrics/metrics", nil)
	metrics.Handler().ServeHTTP(recorder, request)
	if recorder.Code != 200 {
		t.Fatalf("status = %d", recorder.Code)
	}
	body := recorder.Body.String()
	wantTypes := map[string]string{
		"web_requests_total":            "counter",
		"web_backend_calls_total":       "counter",
		"web_backend_tokens_total":      "counter",
		"web_key_rotations_total":       "counter",
		"web_cache_bytes_written_total": "counter",
		"web_request_duration_seconds":  "histogram",
	}
	gotTypes := make(map[string]string)
	typeLines := 0
	for _, forbidden := range []string{"workspace", "session", "thread", "token", "url"} {
		if strings.Contains(body, forbidden+"=") {
			t.Errorf("metric output contains forbidden label %q", forbidden)
		}
	}
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "# TYPE ") {
			typeLines++
			var family, metricType string
			if _, err := fmt.Sscanf(line, "# TYPE %s %s", &family, &metricType); err != nil {
				t.Fatalf("parse type line %q: %v", line, err)
			}
			gotTypes[family] = metricType
		}
	}
	if fmt.Sprint(gotTypes) != fmt.Sprint(wantTypes) {
		t.Fatalf("type families=%v want=%v\n%s", gotTypes, wantTypes, body)
	}
	if typeLines != len(wantTypes) {
		t.Fatalf("type declarations=%d want=%d\n%s", typeLines, len(wantTypes), body)
	}
	for _, required := range []string{
		`web_requests_total{operation="open",status="completed"} 2`,
		`web_backend_calls_total{api="reader",outcome="success"} 1`,
		`web_backend_tokens_total{api="reader"} 765`,
		`web_key_rotations_total{reason="429"} 1`,
		`web_cache_bytes_written_total 42`,
		`web_request_duration_seconds_bucket{operation="open",le="0.005"} 1`,
		`web_request_duration_seconds_bucket{operation="open",le="0.025"} 2`,
		`web_request_duration_seconds_bucket{operation="open",le="+Inf"} 2`,
		`web_request_duration_seconds_count{operation="open"} 2`,
		`web_request_duration_seconds_sum{operation="open"}`,
	} {
		if !strings.Contains(body, required) {
			t.Errorf("missing sample %s", required)
		}
	}
	wantLabels := map[string][]string{
		"web_requests_total":                  {"operation", "status"},
		"web_backend_calls_total":             {"api", "outcome"},
		"web_backend_tokens_total":            {"api"},
		"web_key_rotations_total":             {"reason"},
		"web_cache_bytes_written_total":       {},
		"web_request_duration_seconds_bucket": {"le", "operation"},
		"web_request_duration_seconds_count":  {"operation"},
		"web_request_duration_seconds_sum":    {"operation"},
	}
	for _, line := range strings.Split(body, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, labels := metricSampleShape(t, line)
		want, ok := wantLabels[name]
		if !ok {
			t.Errorf("unexpected metric sample family %q in %q", name, line)
			continue
		}
		if fmt.Sprint(labels) != fmt.Sprint(want) {
			t.Errorf("labels for %s=%v want=%v", name, labels, want)
		}
	}
}

func metricSampleShape(t *testing.T, line string) (string, []string) {
	t.Helper()
	nameEnd := strings.IndexAny(line, "{ ")
	if nameEnd < 0 {
		t.Fatalf("invalid metric sample %q", line)
	}
	name := line[:nameEnd]
	if line[nameEnd] == ' ' {
		return name, nil
	}
	labelsEnd := strings.Index(line[nameEnd:], "}")
	if labelsEnd < 0 {
		t.Fatalf("invalid metric labels %q", line)
	}
	fields := strings.Split(line[nameEnd+1:nameEnd+labelsEnd], ",")
	labels := make([]string, 0, len(fields))
	for _, field := range fields {
		name, _, ok := strings.Cut(field, "=")
		if !ok {
			t.Fatalf("invalid metric label %q", field)
		}
		labels = append(labels, name)
	}
	sort.Strings(labels)
	return name, labels
}
