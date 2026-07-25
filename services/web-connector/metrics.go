package webconnector

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

type metricKey struct{ first, second string }
type durationRecord struct {
	buckets []uint64
	count   uint64
	sum     float64
}

type durationBucket struct {
	label      string
	upperBound float64
}

var requestDurationBuckets = []durationBucket{
	{label: "0.005", upperBound: 0.005},
	{label: "0.01", upperBound: 0.01},
	{label: "0.025", upperBound: 0.025},
	{label: "0.05", upperBound: 0.05},
	{label: "0.1", upperBound: 0.1},
	{label: "0.25", upperBound: 0.25},
	{label: "0.5", upperBound: 0.5},
	{label: "1", upperBound: 1},
	{label: "2.5", upperBound: 2.5},
	{label: "5", upperBound: 5},
	{label: "10", upperBound: 10},
}

func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		m.mu.Lock()
		defer m.mu.Unlock()
		var b strings.Builder
		b.WriteString("# HELP web_requests_total Web requests.\n# TYPE web_requests_total counter\n")
		for _, key := range sortedMetricKeys(m.requests) {
			fmt.Fprintf(&b, "web_requests_total{operation=%q,status=%q} %d\n", key.first, key.second, m.requests[key])
		}
		b.WriteString("# HELP web_backend_calls_total Web backend calls.\n# TYPE web_backend_calls_total counter\n")
		for _, key := range sortedMetricKeys(m.backendCalls) {
			fmt.Fprintf(&b, "web_backend_calls_total{api=%q,outcome=%q} %d\n", key.first, key.second, m.backendCalls[key])
		}
		b.WriteString("# HELP web_backend_tokens_total Web backend tokens.\n# TYPE web_backend_tokens_total counter\n")
		for _, key := range sortedStringKeys(m.backendTokens) {
			fmt.Fprintf(&b, "web_backend_tokens_total{api=%q} %d\n", key, m.backendTokens[key])
		}
		b.WriteString("# HELP web_key_rotations_total Web key rotations.\n# TYPE web_key_rotations_total counter\n")
		for _, key := range sortedStringKeys(m.rotations) {
			fmt.Fprintf(&b, "web_key_rotations_total{reason=%q} %d\n", key, m.rotations[key])
		}
		b.WriteString("# HELP web_cache_bytes_written_total Web cache bytes written.\n# TYPE web_cache_bytes_written_total counter\n")
		fmt.Fprintf(&b, "web_cache_bytes_written_total %d\n", m.cacheBytes)
		b.WriteString("# HELP web_request_duration_seconds Web request duration.\n# TYPE web_request_duration_seconds histogram\n")
		for _, operation := range sortedDurationKeys(m.durations) {
			record := m.durations[operation]
			for index, bucket := range requestDurationBuckets {
				var count uint64
				if index < len(record.buckets) {
					count = record.buckets[index]
				}
				fmt.Fprintf(&b, "web_request_duration_seconds_bucket{operation=%q,le=%q} %d\n", operation, bucket.label, count)
			}
			fmt.Fprintf(&b, "web_request_duration_seconds_bucket{operation=%q,le=\"+Inf\"} %d\n", operation, record.count)
			fmt.Fprintf(&b, "web_request_duration_seconds_sum{operation=%q} %g\nweb_request_duration_seconds_count{operation=%q} %d\n", operation, record.sum, operation, record.count)
		}
		_, _ = w.Write([]byte(b.String()))
	})
}

func sortedMetricKeys(values map[metricKey]uint64) []metricKey {
	keys := make([]metricKey, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].first == keys[j].first {
			return keys[i].second < keys[j].second
		}
		return keys[i].first < keys[j].first
	})
	return keys
}
func sortedStringKeys(values map[string]uint64) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
func sortedDurationKeys(values map[string]durationRecord) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

type Metrics struct {
	mu            sync.Mutex
	requests      map[metricKey]uint64
	backendCalls  map[metricKey]uint64
	backendTokens map[string]uint64
	rotations     map[string]uint64
	cacheBytes    uint64
	durations     map[string]durationRecord
}

func NewMetrics() *Metrics {
	return &Metrics{requests: map[metricKey]uint64{}, backendCalls: map[metricKey]uint64{}, backendTokens: map[string]uint64{}, rotations: map[string]uint64{}, durations: map[string]durationRecord{}}
}
func (m *Metrics) ObserveRequest(operation, status string, duration time.Duration) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests[metricKey{operation, status}]++
	r := m.durations[operation]
	if len(r.buckets) != len(requestDurationBuckets) {
		r.buckets = make([]uint64, len(requestDurationBuckets))
	}
	seconds := duration.Seconds()
	for index, bucket := range requestDurationBuckets {
		if seconds <= bucket.upperBound {
			r.buckets[index]++
		}
	}
	r.count++
	r.sum += seconds
	m.durations[operation] = r
}
func (m *Metrics) ObserveBackend(api, outcome string, tokens int64) {
	m.ObserveBackendCall(api, outcome)
	m.ObserveBackendTokens(api, tokens)
}
func (m *Metrics) ObserveBackendCall(api, outcome string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.backendCalls[metricKey{api, outcome}]++
}
func (m *Metrics) ObserveBackendTokens(api string, tokens int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if tokens > 0 {
		m.backendTokens[api] += uint64(tokens)
	}
}
func (m *Metrics) ObserveRotation(reason string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rotations[reason]++
}
func (m *Metrics) ObserveCacheBytes(value int64) {
	if m == nil || value <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cacheBytes += uint64(value)
}
