package jobrunner

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestRuntimePlacementZeroValueMetricsConcurrentCollectorAndHooks(t *testing.T) {
	var metrics RuntimePlacementMetrics
	var joined sync.WaitGroup
	for range 8 {
		joined.Add(1)
		go func() {
			defer joined.Done()
			for range 100 {
				metrics.observeProbe("eligible", 250*time.Millisecond)
				metrics.observeAttempt("selected", 250*time.Millisecond)
				if _, err := metrics.Collector()(context.Background()); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	joined.Wait()
	samples, err := metrics.Collector()(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	counts, sums := map[string]float64{}, map[string]float64{}
	for _, sample := range samples {
		if sample.Name == "runtime_placement_probe_seconds_total" || sample.Name == "runtime_placement_seconds_total" {
			if sample.Value != 200 {
				t.Fatalf("legacy sum changed: %+v", sample)
			}
		}
		for _, label := range sample.Labels {
			if label.Name == "operation" {
				if sample.Name == "tetral_operation_duration_seconds_count" {
					counts[label.Value] += sample.Value
				}
				if sample.Name == "tetral_operation_duration_seconds_sum" {
					sums[label.Value] += sample.Value
				}
			}
		}
	}
	for _, operation := range []string{"runtime_placement_probe", "runtime_placement"} {
		if counts[operation] != 800 || sums[operation] != 200 {
			t.Fatalf("concurrent owning population %s=%v/%v", operation, counts[operation], sums[operation])
		}
	}
}
