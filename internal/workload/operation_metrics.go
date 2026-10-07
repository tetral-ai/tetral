package workload

import (
	"context"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// operationDurationBuckets are seconds upper bounds. The TypeScript projection
// is checked against this domain by the owning conformance test.
var operationDurationBuckets = [...]float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300, 900, 1800}

type operationKey struct{ operation, outcome string }
type operationObservation struct {
	count   uint64
	sum     float64
	buckets [len(operationDurationBuckets)]uint64
}

// OperationMetrics retains fixed-bucket observations only for a constructor's
// static operation domain. Arbitrary method input maps to unknown_method.
type OperationMetrics struct {
	mu         sync.Mutex
	service    string
	operations map[string]bool
	records    map[operationKey]operationObservation
}

// NewOperationMetrics constructs one owner's registry. Callers pass their
// owning service constant; a name outside the closed workload domain is a
// startup programming error, so it panics instead of relabeling samples. Only
// the shared HTTP and gRPC shutdown phases are registered here; an owner adds
// its own phases, such as Queue's drain, through SetOperations.
func NewOperationMetrics(service string, operations ...string) *OperationMetrics {
	switch service {
	case "api", "auth", "queue", "bridge", "sandbox", "job-runner", "event-stream", "web-connector", "git-proxy", "cleanup":
	default:
		panic("workload: operation metrics service " + strconv.Quote(service) + " is outside the closed workload domain")
	}
	m := &OperationMetrics{service: service, operations: map[string]bool{}, records: map[operationKey]operationObservation{}}
	for _, operation := range append(operations, "shutdown_http_drain", "shutdown_http_join", "shutdown_grpc_drain", "shutdown_grpc_cancel_join") {
		m.operations[operation] = true
	}
	return m
}

// SetOperations installs the registered RPC descriptor's closed method domain,
// before listener admission. This does not derive labels from request payloads.
func (m *OperationMetrics) SetOperations(operations []string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, operation := range operations {
		m.operations[operation] = true
	}
}

func (m *OperationMetrics) Observe(operation, outcome string, duration time.Duration) {
	m.record(operation, outcome, duration)
}

// ObserveShutdown records the same completed owning phase in the public
// histogram and the process diagnostic stream. The metrics listener may already
// be closed; diagnostic delivery remains best effort and never owns the join.
func (m *OperationMetrics) ObserveShutdown(logger *slog.Logger, operation, outcome string, duration time.Duration) {
	operation, outcome, count := m.record(operation, outcome, duration)
	if count == 0 || logger == nil {
		return
	}
	// Custom sinks are allowed in reusable runners. A throwing sink must not
	// change a completed shutdown phase or prevent subsequent resource cleanup.
	defer func() { _ = recover() }()
	logger.Info("workload.shutdown.phase_completed", "operation", operation, "outcome", outcome,
		"component", "workload", "duration.seconds", duration.Seconds(), "metric.observation.count", count)
}

func (m *OperationMetrics) record(operation, outcome string, duration time.Duration) (string, string, uint64) {
	if m == nil || duration < 0 {
		return "", "", 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.operations[operation] {
		operation = "unknown_method"
	}
	switch outcome {
	case "success", "error", "cancelled", "rejected", "timeout":
	default:
		outcome = "error"
	}
	key := operationKey{operation, outcome}
	value := m.records[key]
	value.count++
	value.sum += duration.Seconds()
	for i, bound := range operationDurationBuckets {
		if duration.Seconds() <= bound {
			value.buckets[i]++
		}
	}
	m.records[key] = value
	return operation, outcome, value.count
}

func (m *OperationMetrics) Collector() MetricsCollector {
	return func(context.Context) ([]Metric, error) {
		if m == nil {
			return nil, nil
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		keys := make([]operationKey, 0, len(m.records))
		for key := range m.records {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].operation == keys[j].operation {
				return keys[i].outcome < keys[j].outcome
			}
			return keys[i].operation < keys[j].operation
		})
		var metrics []Metric
		const name = "tetral_operation_duration_seconds"
		for _, key := range keys {
			value := m.records[key]
			labels := []MetricLabel{{"service", m.service}, {"operation", key.operation}, {"outcome", key.outcome}}
			appendSample := func(suffix string, labels []MetricLabel, value float64) {
				metrics = append(metrics, Metric{Name: name + suffix, Family: name, Help: "Completed owning operation duration in seconds.", Type: "histogram", Labels: labels, Value: value})
			}
			for i, bound := range operationDurationBuckets {
				appendSample("_bucket", append(append([]MetricLabel(nil), labels...), MetricLabel{"le", strconv.FormatFloat(bound, 'g', -1, 64)}), float64(value.buckets[i]))
			}
			appendSample("_bucket", append(append([]MetricLabel(nil), labels...), MetricLabel{"le", "+Inf"}), float64(value.count))
			appendSample("_count", labels, float64(value.count))
			appendSample("_sum", labels, value.sum)
		}
		return metrics, nil
	}
}

func grpcMetricOutcome(code string) string {
	switch code {
	case "OK":
		return "success"
	case "Canceled":
		return "cancelled"
	case "DeadlineExceeded":
		return "timeout"
	case "InvalidArgument", "Unauthenticated", "PermissionDenied", "ResourceExhausted", "Unimplemented":
		return "rejected"
	default:
		return "error"
	}
}

// Text renders only this histogram family for owners with custom metrics ports.
func (m *OperationMetrics) Text() string {
	samples, _ := m.Collector()(context.Background())
	var builder strings.Builder
	headers := map[string]bool{}
	for _, sample := range samples {
		writeMetric(&builder, headers, sample)
	}
	return builder.String()
}
