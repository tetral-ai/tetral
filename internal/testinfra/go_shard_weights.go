package testinfra

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"
)

//go:embed go_shard_weights.json
var goShardWeightsJSON []byte

const goShardWeightsVersion = 2

// goShardWeights holds medians of Go Race evidence, written by
// tetral-shard-weights in integer milliseconds. A Go step costs its package's
// overhead (step time outside its units, mostly compile) plus its units: the
// package's Elapsed when the package runs whole, or the active times of its
// top-level tests when a sliceable package is split by test. Every sample is
// first divided by its job's CPU model factor, so all weights are on one
// machine scale: the median source job's.
type goShardWeights struct {
	Version     int    `json:"version"`
	GeneratedAt string `json:"generated_at"`
	// Base is the commit every source run's tested commit contains.
	Base       string   `json:"base"`
	SourceRuns []string `json:"source_runs"`
	// MedianTestMS is the median active time of every top-level test in the
	// source runs; it prices tests of packages the file does not know.
	MedianTestMS int64 `json:"median_test_ms"`
	// Sliceable lists the packages split across shards by top-level test.
	// It is an explicit choice; the generator keeps it as it finds it.
	Sliceable []string                    `json:"sliceable"`
	Packages  map[string]goPackageWeights `json:"packages"`
	// CPUModels holds the fitted factor of every runner CPU model in the
	// source runs and the evidence behind it.
	CPUModels map[string]goCPUModelWeights `json:"cpu_models"`

	sliceable  map[string]bool
	testMedian map[string]int64
}

// goCPUModelWeights is a CPU model's speed against the weights' scale: its
// jobs' samples divided by Factor are on that scale. Samples counts the fit
// samples its jobs gave, and SharedUnits the fit units also measured on
// another model, which tie its factor to theirs.
type goCPUModelWeights struct {
	Factor      float64 `json:"factor"`
	Jobs        int     `json:"jobs"`
	Samples     int     `json:"samples"`
	SharedUnits int     `json:"shared_units"`
}

type goPackageWeights struct {
	ElapsedMS  int64            `json:"elapsed_ms"`
	OverheadMS int64            `json:"overhead_ms"`
	TestsMS    map[string]int64 `json:"tests_ms,omitempty"`
}

func loadGoShardWeights() (goShardWeights, error) {
	return parseGoShardWeights(goShardWeightsJSON)
}

func parseGoShardWeights(body []byte) (goShardWeights, error) {
	var value goShardWeights
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return goShardWeights{}, fmt.Errorf("decode Go shard weights: %w", err)
	}
	if value.Version != goShardWeightsVersion || len(value.Packages) == 0 || len(value.SourceRuns) == 0 || value.Base == "" {
		return goShardWeights{}, fmt.Errorf("unsupported or incomplete Go shard weights")
	}
	if _, err := time.Parse(time.DateOnly, value.GeneratedAt); err != nil {
		return goShardWeights{}, fmt.Errorf("go shard weights generation date is malformed")
	}
	for _, run := range value.SourceRuns {
		if _, err := strconv.ParseUint(run, 10, 64); err != nil {
			return goShardWeights{}, fmt.Errorf("go shard weights source run %q is malformed", run)
		}
	}
	if value.MedianTestMS < 0 {
		return goShardWeights{}, fmt.Errorf("go shard weights median test weight is negative")
	}
	if len(value.CPUModels) == 0 {
		return goShardWeights{}, fmt.Errorf("go shard weights record no CPU model factors")
	}
	for model, item := range value.CPUModels {
		if model == "" || !(item.Factor > 0) {
			return goShardWeights{}, fmt.Errorf("invalid CPU model factor for %q", model)
		}
	}
	value.sliceable = map[string]bool{}
	for _, name := range value.Sliceable {
		if name == "" || value.sliceable[name] {
			return goShardWeights{}, fmt.Errorf("invalid or duplicate sliceable Go package %q", name)
		}
		value.sliceable[name] = true
	}
	value.testMedian = map[string]int64{}
	for name, item := range value.Packages {
		if name == "" || item.ElapsedMS < 0 || item.OverheadMS < 0 {
			return goShardWeights{}, fmt.Errorf("invalid Go shard weights for package %q", name)
		}
		if len(item.TestsMS) > 0 && !value.sliceable[name] {
			return goShardWeights{}, fmt.Errorf("go package %q has test weights but is not sliceable", name)
		}
		for test, weight := range item.TestsMS {
			if test == "" || weight < 0 {
				return goShardWeights{}, fmt.Errorf("invalid Go shard weight for test %q of %q", test, name)
			}
		}
		if len(item.TestsMS) > 0 {
			value.testMedian[name] = medianMilliseconds(slices.Collect(maps.Values(item.TestsMS)))
		}
	}
	return value, nil
}

// overhead is charged once for every step of the package. A package the
// weights do not know has none: its fallback weight covers the whole step.
func (weights goShardWeights) overhead(name string) int64 {
	return weights.Packages[name].OverheadMS
}

// test prices one top-level test of a sliceable package. A test the weights
// do not know costs its package's median test weight, or the repository's
// when the package is unknown too.
func (weights goShardWeights) test(name, test string) int64 {
	if weight, ok := weights.Packages[name].TestsMS[test]; ok {
		return weight
	}
	if median, ok := weights.testMedian[name]; ok {
		return median
	}
	return weights.MedianTestMS
}

// selection prices one Go step: its package's overhead plus either its tests
// (a sliceable package) or the package's Elapsed. A package the weights do
// not know costs its test count times the repository median test weight.
func (weights goShardWeights) selection(selection Selection) int64 {
	name := selection.Packages[0]
	if weights.sliceable[name] && len(selection.Tests) > 0 {
		total := weights.overhead(name)
		for _, test := range selection.Tests {
			total += weights.test(name, test)
		}
		return total
	}
	if item, ok := weights.Packages[name]; ok {
		return item.OverheadMS + item.ElapsedMS
	}
	return int64(len(selection.Tests)) * weights.MedianTestMS
}

func (weights goShardWeights) cpuFactors() map[string]float64 {
	factors := map[string]float64{}
	for model, item := range weights.CPUModels {
		factors[model] = item.Factor
	}
	return factors
}

// totalTestWork sums every package's median Elapsed.
func (weights goShardWeights) totalTestWork() time.Duration {
	var total int64
	for _, item := range weights.Packages {
		total += item.ElapsedMS
	}
	return time.Duration(total) * time.Millisecond
}

func medianMilliseconds(values []int64) int64 {
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	middle := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[middle]
	}
	return (sorted[middle-1] + sorted[middle] + 1) / 2
}
