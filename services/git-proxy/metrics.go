package gitproxy

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

type GitProxyMetrics struct {
	mu               sync.Mutex
	active           int64
	bytesIn          int64
	bytesOut         int64
	requests         map[requestMetricKey]uint64
	latencyBuckets   []uint64
	latencyCount     uint64
	latencySum       float64
	ticketRejections map[string]uint64
}

type requestMetricKey struct {
	endpoint       string
	decision       string
	upstreamStatus int
}

func NewGitProxyMetrics() *GitProxyMetrics {
	return &GitProxyMetrics{
		requests:         make(map[requestMetricKey]uint64),
		latencyBuckets:   make([]uint64, len(gitProxyLatencyBuckets)),
		ticketRejections: map[string]uint64{"malformed": 0, "unauthorized": 0},
	}
}

func (m *GitProxyMetrics) IncActive() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.active++
}

func (m *GitProxyMetrics) DecActive() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active > 0 {
		m.active--
	}
}

func (m *GitProxyMetrics) ObserveRequest(endpoint string, decision string, upstreamStatus int, bytesIn int64, bytesOut int64, duration time.Duration) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests[requestMetricKey{endpoint: endpoint, decision: decision, upstreamStatus: upstreamStatus}]++
	m.bytesIn += bytesIn
	m.bytesOut += bytesOut
	seconds := duration.Seconds()
	if len(m.latencyBuckets) != len(gitProxyLatencyBuckets) {
		m.latencyBuckets = make([]uint64, len(gitProxyLatencyBuckets))
	}
	for index, bucket := range gitProxyLatencyBuckets {
		if seconds <= bucket.upperBound {
			m.latencyBuckets[index]++
		}
	}
	m.latencyCount++
	m.latencySum += seconds
}

func (m *GitProxyMetrics) ObserveTicketRejection(reason string) {
	if m == nil {
		return
	}
	if reason == "" {
		reason = "unauthorized"
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ticketRejections == nil {
		m.ticketRejections = make(map[string]uint64)
	}
	m.ticketRejections[reason]++
}

func (m *GitProxyMetrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	if m == nil {
		m = NewGitProxyMetrics()
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(m.render()))
}

func (m *GitProxyMetrics) render() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var b strings.Builder
	b.WriteString("# TYPE gitproxy_active_connections gauge\n")
	fmt.Fprintf(&b, "gitproxy_active_connections %d\n", m.active)
	b.WriteString("# TYPE gitproxy_bytes_relayed_total counter\n")
	fmt.Fprintf(&b, "gitproxy_bytes_relayed_total{direction=\"in\"} %d\n", m.bytesIn)
	fmt.Fprintf(&b, "gitproxy_bytes_relayed_total{direction=\"out\"} %d\n", m.bytesOut)
	b.WriteString("# TYPE gitproxy_requests_total counter\n")
	if len(m.requests) == 0 {
		b.WriteString("gitproxy_requests_total{endpoint=\"\",decision=\"\",upstream_status=\"0\"} 0\n")
	} else {
		keys := make([]requestMetricKey, 0, len(m.requests))
		for key := range m.requests {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].endpoint != keys[j].endpoint {
				return keys[i].endpoint < keys[j].endpoint
			}
			if keys[i].decision != keys[j].decision {
				return keys[i].decision < keys[j].decision
			}
			return keys[i].upstreamStatus < keys[j].upstreamStatus
		})
		for _, key := range keys {
			fmt.Fprintf(&b, "gitproxy_requests_total{endpoint=\"%s\",decision=\"%s\",upstream_status=\"%d\"} %d\n",
				metricLabel(key.endpoint), metricLabel(key.decision), key.upstreamStatus, m.requests[key])
		}
	}
	b.WriteString("# TYPE gitproxy_upstream_latency_seconds histogram\n")
	for index, bucket := range gitProxyLatencyBuckets {
		var count uint64
		if index < len(m.latencyBuckets) {
			count = m.latencyBuckets[index]
		}
		fmt.Fprintf(&b, "gitproxy_upstream_latency_seconds_bucket{le=\"%s\"} %d\n", bucket.label, count)
	}
	fmt.Fprintf(&b, "gitproxy_upstream_latency_seconds_bucket{le=\"+Inf\"} %d\n", m.latencyCount)
	fmt.Fprintf(&b, "gitproxy_upstream_latency_seconds_sum %.6f\n", m.latencySum)
	fmt.Fprintf(&b, "gitproxy_upstream_latency_seconds_count %d\n", m.latencyCount)
	b.WriteString("# TYPE gitproxy_ticket_rejections_total counter\n")
	writeStringCounterMap(&b, "gitproxy_ticket_rejections_total", "reason", m.ticketRejections)
	b.WriteString("# CONTRACT gitproxy_alert_condition ")
	b.WriteString(AlertTicketRejectionSpike)
	b.WriteString("\n")
	b.WriteString("# CONTRACT gitproxy_alert_condition ")
	b.WriteString(AlertUpstream5xxRatio)
	b.WriteString("\n")
	return b.String()
}

type gitProxyLatencyBucket struct {
	label      string
	upperBound float64
}

var gitProxyLatencyBuckets = []gitProxyLatencyBucket{
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

func writeStringCounterMap(b *strings.Builder, metric string, label string, values map[string]uint64) {
	if len(values) == 0 {
		fmt.Fprintf(b, "%s{%s=\"\"} 0\n", metric, label)
		return
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintf(b, "%s{%s=\"%s\"} %d\n", metric, label, metricLabel(key), values[key])
	}
}

func metricLabel(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	return strings.ReplaceAll(value, `"`, `\"`)
}
