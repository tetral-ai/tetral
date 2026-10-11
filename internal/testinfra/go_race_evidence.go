package testinfra

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// goRaceRun is one workflow run's downloaded Go Race evidence: the latest
// attempt of each shard. A rerun of failed jobs leaves the earlier attempts'
// results beside the new ones.
type goRaceRun struct {
	runID string
	// shardCount is the count every shard's identity records; 0 when the
	// results predate shard identities.
	shardCount int
	shards     map[int]*benchmarkShardEvidence
	attempts   map[int]uint64
}

// goRaceShardIndex identifies a Go Race shard result by its recorded shard
// identity. Results from runners that recorded none name their shard only in
// the producer, go-0 … go-(K-1); go-static is not one.
func goRaceShardIndex(result Result) (index, count int, ok bool) {
	if identity := result.Plan.GoSelection; identity != nil {
		return identity.ShardIndex, identity.ShardCount, true
	}
	digits, found := strings.CutPrefix(result.Execution.Producer, "go-")
	if !found {
		return 0, 0, false
	}
	index, err := strconv.Atoi(digits)
	if err != nil || index < 0 || strconv.Itoa(index) != digits {
		return 0, 0, false
	}
	return index, 0, true
}

// loadGoRaceRuns reads every Go Race result.json beneath root, grouped by run.
func loadGoRaceRuns(root string) (map[string]*goRaceRun, error) {
	runs := map[string]*goRaceRun{}
	err := filepath.WalkDir(root, func(file string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Name() != "result.json" {
			return nil
		}
		// Results are discovered beneath the caller's downloaded evidence root.
		//nolint:gosec
		body, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		var result Result
		if err := json.Unmarshal(body, &result); err != nil {
			return fmt.Errorf("malformed result %s: %w", file, err)
		}
		index, count, ok := goRaceShardIndex(result)
		if !ok {
			return nil
		}
		run := runs[result.Execution.RunID]
		if run == nil {
			run = &goRaceRun{runID: result.Execution.RunID, shardCount: count, shards: map[int]*benchmarkShardEvidence{}, attempts: map[int]uint64{}}
			runs[run.runID] = run
		}
		if count != run.shardCount {
			return fmt.Errorf("run %s records shard counts %d and %d", run.runID, run.shardCount, count)
		}
		attempt, _ := strconv.ParseUint(result.Execution.RunAttempt, 10, 32)
		if previous, exists := run.attempts[index]; exists {
			if previous == attempt {
				return fmt.Errorf("run %s holds two results for shard %d attempt %d", run.runID, index, attempt)
			}
			if previous > attempt {
				return nil
			}
		}
		shard, err := loadBenchmarkShard(filepath.Dir(file), result)
		if err != nil {
			return fmt.Errorf("run %s shard %d: %w", run.runID, index, err)
		}
		run.attempts[index] = attempt
		run.shards[index] = shard
		return nil
	})
	return runs, err
}

func (run *goRaceRun) shardIndexes() []int {
	return slices.Sorted(maps.Keys(run.shards))
}

// complete reports whether shards 0..K-1 are all present and each ran every
// planned step. Results without shard identities cannot show K, so a missing
// last shard goes unnoticed in them.
func (run *goRaceRun) complete() bool {
	indexes := run.shardIndexes()
	if run.shardCount > 0 && len(indexes) != run.shardCount {
		return false
	}
	for position, index := range indexes {
		if index != position || !run.shards[index].complete {
			return false
		}
	}
	return len(indexes) > 0
}

// splitPackages lists the packages the run's plan split into more than one
// step.
func (run *goRaceRun) splitPackages() map[string]bool {
	steps := map[string]int{}
	for _, shard := range run.shards {
		for _, selection := range shard.plan {
			steps[selection.Packages[0]]++
		}
	}
	split := map[string]bool{}
	for name, count := range steps {
		if count > 1 {
			split[name] = true
		}
	}
	return split
}

const (
	// A job's speed uses only units the weights give at least this long and
	// needs this many of them; with fewer it has none.
	goRaceJobSpeedFloorMS      = 1000
	goRaceJobSpeedMinimumUnits = 5
)

// goRaceJobSpeed is a Go Race job's speed against the shard weights: the
// median, over the job's passed units that the weights give at least a
// second, of measured ÷ weight. Units are priced as the weights price them:
// a sliceable package's tests by active time, any other package by its
// Elapsed, unless the job's run split that package, which leaves no whole
// measurement. Above 1 the job ran slower than the weights.
func goRaceJobSpeed(shard *benchmarkShardEvidence, split map[string]bool, weights goShardWeights) (speed float64, units int, ok bool) {
	var ratios []float64
	for _, step := range shard.steps {
		name := step.selection.Packages[0]
		if weights.sliceable[name] {
			for _, test := range step.report.tests {
				if weight, weighed := weights.Packages[name].TestsMS[test.name]; weighed && test.passed && weight >= goRaceJobSpeedFloorMS {
					ratios = append(ratios, test.active*1000/float64(weight))
				}
			}
			continue
		}
		item, known := weights.Packages[name]
		if known && !split[name] && item.ElapsedMS >= goRaceJobSpeedFloorMS && step.result.Status == "pass" && step.report.packageAction == "pass" {
			ratios = append(ratios, step.report.packageElapsed*1000/float64(item.ElapsedMS))
		}
	}
	if len(ratios) < goRaceJobSpeedMinimumUnits {
		return 0, len(ratios), false
	}
	return benchmarkMedian(ratios), len(ratios), true
}
