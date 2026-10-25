package workload

import (
	"context"
	"log/slog"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type operationKey struct{ operation, outcome string }
type operationObservation struct {
	long    bool
	count   uint64
	sum     float64
	buckets []uint64
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
	m.addOperations(append(operations, "shutdown_http_drain", "shutdown_http_join", "shutdown_grpc_drain", "shutdown_grpc_cancel_join"))
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
	m.addOperations(operations)
}

// addOperations admits operations to the label domain; once the service has
// registered its table, each must be declared there or in the library's.
func (m *OperationMetrics) addOperations(operations []string) {
	registered := RegisteredDeclarations(m.service) != nil
	for _, operation := range operations {
		if _, declared := lookupOperation(m.service, operation); registered && !declared {
			undeclared("operation", operation)
		}
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
	declared, _ := lookupOperation(m.service, operation)
	// The outcome domain is Observe's: any other outcome must be a code the
	// operation declares.
	switch {
	case outcome == "success":
		outcome = "ok"
	case outcome == "ok", outcome == "error", outcome == "rejected", slices.Contains(declared.Errors, outcome):
	default:
		outcome = "error"
	}
	key := operationKey{operation, outcome}
	value, seen := m.records[key]
	if !seen {
		value.long = declared.Long
		value.buckets = make([]uint64, len(operationBuckets(value.long)))
	}
	value.count++
	value.sum += duration.Seconds()
	for i, bound := range operationBuckets(value.long) {
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
		for _, key := range keys {
			value := m.records[key]
			name, help := "tetral_operation_duration_seconds", "Duration of a completed operation."
			if value.long {
				name, help = "tetral_long_operation_duration_seconds", "Duration of a completed long operation."
			}
			labels := []MetricLabel{{"service", m.service}, {"operation", key.operation}, {"outcome", key.outcome}}
			appendSample := func(suffix string, labels []MetricLabel, value float64) {
				metrics = append(metrics, Metric{Name: name + suffix, Family: name, Help: help, Type: "histogram", Labels: labels, Value: value})
			}
			for i, bound := range operationBuckets(value.long) {
				appendSample("_bucket", append(append([]MetricLabel(nil), labels...), MetricLabel{"le", strconv.FormatFloat(bound, 'g', -1, 64)}), float64(value.buckets[i]))
			}
			appendSample("_bucket", append(append([]MetricLabel(nil), labels...), MetricLabel{"le", "+Inf"}), float64(value.count))
			appendSample("_count", labels, float64(value.count))
			appendSample("_sum", labels, value.sum)
		}
		return metrics, nil
	}
}

// operationBuckets is the bucket set of an operation that its table marks
// long or not.
func operationBuckets(long bool) []float64 {
	if long {
		return LongOperationDurationBuckets
	}
	return RequestDurationBuckets
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
