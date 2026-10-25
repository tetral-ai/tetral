package workload

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// RequestDurationBuckets and LongOperationDurationBuckets are the only two
// bucket sets a duration histogram may use, in seconds.
var (
	RequestDurationBuckets       = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	LongOperationDurationBuckets = []float64{0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800}
)

// Registry holds metric families whose label values come from sets fixed in
// code. Registering a name twice returns the first family when its shape is
// the same and panics otherwise: one name has one meaning per process.
type Registry struct {
	mu       sync.Mutex
	families map[string]*metricFamily
	order    []string
}

type metricFamily struct {
	name, help, kind string
	labels           []string
	buckets          []float64
	series           map[string]*metricSeries
}

type metricSeries struct {
	labels  []string
	value   float64
	count   uint64
	sum     float64
	buckets []uint64
}

// Counter, Gauge and Histogram are handles to one family of a Registry.
type Counter struct {
	family   *metricFamily
	registry *Registry
}
type Gauge struct {
	family   *metricFamily
	registry *Registry
}
type Histogram struct {
	family   *metricFamily
	registry *Registry
}

func NewRegistry() *Registry { return &Registry{families: map[string]*metricFamily{}} }

func (r *Registry) register(name, help, kind string, buckets []float64, labels []string) *metricFamily {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing := r.families[name]; existing != nil {
		if existing.kind != kind || strings.Join(existing.labels, ",") != strings.Join(labels, ",") || fmt.Sprint(existing.buckets) != fmt.Sprint(buckets) {
			panic("workload: metric family " + strconv.Quote(name) + " registered with two shapes")
		}
		return existing
	}
	family := &metricFamily{name: name, help: help, kind: kind, labels: append([]string(nil), labels...), buckets: append([]float64(nil), buckets...), series: map[string]*metricSeries{}}
	r.families[name] = family
	r.order = append(r.order, name)
	return family
}

func (r *Registry) Counter(name, help string, labels ...string) *Counter {
	return &Counter{r.register(name, help, "counter", nil, labels), r}
}

func (r *Registry) Gauge(name, help string, labels ...string) *Gauge {
	return &Gauge{r.register(name, help, "gauge", nil, labels), r}
}

func (r *Registry) Histogram(name, help string, buckets []float64, labels ...string) *Histogram {
	return &Histogram{r.register(name, help, "histogram", buckets, labels), r}
}

func (r *Registry) seriesFor(family *metricFamily, values []string) *metricSeries {
	if len(values) != len(family.labels) {
		panic("workload: metric family " + strconv.Quote(family.name) + " observed with the wrong label count")
	}
	key := strings.Join(values, "\x00")
	s := family.series[key]
	if s == nil {
		s = &metricSeries{labels: append([]string(nil), values...)}
		if family.kind == "histogram" {
			s.buckets = make([]uint64, len(family.buckets))
		}
		family.series[key] = s
	}
	return s
}

func (c *Counter) Add(delta float64, labelValues ...string) {
	if c == nil || delta < 0 {
		return
	}
	c.registry.mu.Lock()
	defer c.registry.mu.Unlock()
	c.registry.seriesFor(c.family, labelValues).value += delta
}

func (g *Gauge) Set(value float64, labelValues ...string) {
	if g == nil {
		return
	}
	g.registry.mu.Lock()
	defer g.registry.mu.Unlock()
	g.registry.seriesFor(g.family, labelValues).value = value
}

func (g *Gauge) Add(delta float64, labelValues ...string) {
	if g == nil {
		return
	}
	g.registry.mu.Lock()
	defer g.registry.mu.Unlock()
	g.registry.seriesFor(g.family, labelValues).value += delta
}

// Observe adds one sample. Exact bounds are inclusive; a negative or NaN
// sample is dropped rather than shown as zero.
func (h *Histogram) Observe(value float64, labelValues ...string) {
	if h == nil || value < 0 || value != value {
		return
	}
	h.registry.mu.Lock()
	defer h.registry.mu.Unlock()
	s := h.registry.seriesFor(h.family, labelValues)
	s.count++
	s.sum += value
	for i, bound := range h.family.buckets {
		if value <= bound {
			s.buckets[i]++
		}
	}
}

// Collector renders every family in registration order, series sorted by
// their label values.
func (r *Registry) Collector() MetricsCollector {
	return func(context.Context) ([]Metric, error) {
		if r == nil {
			return nil, nil
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		var metrics []Metric
		for _, name := range r.order {
			family := r.families[name]
			keys := make([]string, 0, len(family.series))
			for key := range family.series {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				s := family.series[key]
				labels := make([]MetricLabel, len(family.labels))
				for i, label := range family.labels {
					labels[i] = MetricLabel{label, s.labels[i]}
				}
				if family.kind != "histogram" {
					metrics = append(metrics, Metric{Name: family.name, Help: family.help, Type: family.kind, Labels: labels, Value: s.value})
					continue
				}
				sample := func(suffix string, labels []MetricLabel, value float64) {
					metrics = append(metrics, Metric{Name: family.name + suffix, Family: family.name, Help: family.help, Type: "histogram", Labels: labels, Value: value})
				}
				for i, bound := range family.buckets {
					sample("_bucket", append(append([]MetricLabel(nil), labels...), MetricLabel{"le", strconv.FormatFloat(bound, 'g', -1, 64)}), float64(s.buckets[i]))
				}
				sample("_bucket", append(append([]MetricLabel(nil), labels...), MetricLabel{"le", "+Inf"}), float64(s.count))
				sample("_count", labels, float64(s.count))
				sample("_sum", labels, s.sum)
			}
		}
		return metrics, nil
	}
}

// Text renders the registry alone, for owners with custom metrics ports.
func (r *Registry) Text() string {
	samples, _ := r.Collector()(context.Background())
	var builder strings.Builder
	headers := map[string]bool{}
	for _, sample := range samples {
		writeMetric(&builder, headers, sample)
	}
	return builder.String()
}
