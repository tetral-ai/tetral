package testinfra

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
)

// goRaceWorkers is the number of Go package processes the CI evidence action
// lets the runner start at once. Plans and replays schedule a shard's steps on
// that many workers in plan order.
const goRaceWorkers = 2

// SelectPlan derives one execution slice from an already reconciled profile.
// It never expands the evidence universe or changes a logical test selection.
func SelectPlan(plan Plan, groups []string, shardIndex, shardCount int) (Plan, error) {
	if shardCount < 1 || shardIndex < 0 || shardIndex >= shardCount {
		return Plan{}, fmt.Errorf("invalid shard %d of %d", shardIndex, shardCount)
	}
	wanted := map[string]bool{}
	for _, group := range groups {
		wanted[group] = true
	}
	if len(wanted) == 0 && (shardCount != 1 || shardIndex != 0) {
		wanted["go"] = true
	}
	selected := plan.Selections[:0:0]
	var goSelections []Selection
	for _, selection := range plan.Selections {
		if len(wanted) > 0 && !wanted[selection.Group] {
			continue
		}
		if selection.Group == "go" {
			goSelections = append(goSelections, selection)
			continue
		}
		selected = append(selected, selection)
	}
	for group := range wanted {
		found := false
		for _, selection := range plan.Selections {
			found = found || selection.Group == group
		}
		if !found {
			return Plan{}, fmt.Errorf("selected evidence group %q is absent from profile", group)
		}
	}
	if len(goSelections) > 0 {
		weights, err := loadGoShardWeights()
		if err != nil {
			return Plan{}, err
		}
		digest, err := goSelectionDigest(goSelections, plan.Excluded)
		if err != nil {
			return Plan{}, err
		}
		shards := balanceGoSelections(goSelections, shardCount, weights)
		selected = append(selected, shards[shardIndex]...)
		plan.GoSelection = &GoShardIdentity{Digest: digest, ShardIndex: shardIndex, ShardCount: shardCount}
	}
	if len(selected) == 0 {
		return Plan{}, fmt.Errorf("selected execution slice is empty")
	}
	plan.Selections = selected
	plan.Dependencies = selectedDependencies(selected)
	return plan, nil
}

// goSelectionDigest hashes a canonical form of a Go selection: every package
// with its sorted tests, and every Go exclusion's package, runnable and
// disposition. Reasons, dependencies, modes, times and revisions are left
// out, so the union of a run's shards hashes like the selection it came from.
// A Go selection names exactly one package; any other shape is rejected.
func goSelectionDigest(selections []Selection, exclusions []Exclusion) (string, error) {
	type packageEntry struct {
		Package string   `json:"package"`
		Tests   []string `json:"tests"`
	}
	type exclusionEntry struct {
		Package     string `json:"package"`
		Runnable    string `json:"runnable"`
		Disposition string `json:"disposition"`
	}
	tests := map[string][]string{}
	for _, selection := range selections {
		if selection.Group != "go" {
			continue
		}
		if len(selection.Packages) != 1 {
			return "", fmt.Errorf("go selection names %d packages; want one", len(selection.Packages))
		}
		tests[selection.Packages[0]] = append(tests[selection.Packages[0]], selection.Tests...)
	}
	document := struct {
		Selections []packageEntry   `json:"selections"`
		Exclusions []exclusionEntry `json:"exclusions"`
	}{Selections: []packageEntry{}, Exclusions: []exclusionEntry{}}
	for _, name := range slices.Sorted(maps.Keys(tests)) {
		sorted := append([]string{}, tests[name]...)
		sort.Strings(sorted)
		document.Selections = append(document.Selections, packageEntry{Package: name, Tests: sorted})
	}
	for _, exclusion := range exclusions {
		if exclusion.Group == "go" {
			document.Exclusions = append(document.Exclusions, exclusionEntry{Package: exclusion.Package, Runnable: exclusion.Runnable, Disposition: exclusion.Disposition})
		}
	}
	sort.Slice(document.Exclusions, func(i, j int) bool {
		left, right := document.Exclusions[i], document.Exclusions[j]
		return left.Package+"\x00"+left.Runnable+"\x00"+left.Disposition < right.Package+"\x00"+right.Runnable+"\x00"+right.Disposition
	})
	// The document holds only strings and slices, which always encode.
	body, _ := json.Marshal(document)
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

// goShardUnit is what the balancer places: a whole package, or one top-level
// test of a sliceable package whose step overhead is charged once per shard.
type goShardUnit struct {
	selection Selection
	weight    int64
	sliced    bool
}

type goShardBuilder struct {
	steps     []Selection
	weights   []int64
	byPackage map[string]int
}

// balanceGoSelections assigns units longest first, each to the shard whose
// simulated makespan is lowest after taking it, the lowest index on a tie. A
// test joining its package's step on a shard adds no second overhead there.
// It returns every shard's steps in planned weight order, the order the
// runner dispatches them in.
func balanceGoSelections(selections []Selection, shardCount int, weights goShardWeights) [][]Selection {
	var units []goShardUnit
	for _, selection := range selections {
		name := selection.Packages[0]
		if shardCount > 1 && weights.sliceable[name] && len(selection.Tests) > 0 {
			for _, test := range selection.Tests {
				part := selection
				part.Tests = []string{test}
				part.Reason = "duration-balanced top-level package slice"
				units = append(units, goShardUnit{selection: part, weight: weights.test(name, test), sliced: true})
			}
			continue
		}
		units = append(units, goShardUnit{selection: selection, weight: weights.selection(selection)})
	}
	sort.Slice(units, func(i, j int) bool {
		if units[i].weight != units[j].weight {
			return units[i].weight > units[j].weight
		}
		return goShardUnitName(units[i]) < goShardUnitName(units[j])
	})
	shards := make([]goShardBuilder, shardCount)
	for index := range shards {
		shards[index].byPackage = map[string]int{}
	}
	for _, unit := range units {
		best, bestStep, bestAdded, bestAfter := 0, -1, int64(0), int64(0)
		for index := range shards {
			step, added := shards[index].cost(unit, weights)
			if after := shards[index].makespanWith(step, added); index == 0 || after < bestAfter {
				best, bestStep, bestAdded, bestAfter = index, step, added, after
			}
		}
		shards[best].add(unit, bestStep, bestAdded)
	}
	result := make([][]Selection, shardCount)
	for index, shard := range shards {
		order := make([]int, len(shard.steps))
		for position := range order {
			order[position] = position
			sort.Strings(shard.steps[position].Tests)
		}
		sort.Slice(order, func(i, j int) bool {
			left, right := order[i], order[j]
			if shard.weights[left] != shard.weights[right] {
				return shard.weights[left] > shard.weights[right]
			}
			return shard.steps[left].Packages[0] < shard.steps[right].Packages[0]
		})
		for _, position := range order {
			result[index] = append(result[index], shard.steps[position])
		}
	}
	return result
}

func goShardUnitName(unit goShardUnit) string {
	return unit.selection.Packages[0] + "\x00" + strings.Join(unit.selection.Tests, "\x00")
}

// cost returns the step a unit joins (-1 for a new step) and the time it adds.
func (shard *goShardBuilder) cost(unit goShardUnit, weights goShardWeights) (int, int64) {
	name := unit.selection.Packages[0]
	if !unit.sliced {
		return -1, unit.weight
	}
	if step, ok := shard.byPackage[name]; ok {
		return step, unit.weight
	}
	return -1, weights.overhead(name) + unit.weight
}

func (shard *goShardBuilder) makespanWith(step int, added int64) int64 {
	durations := slices.Clone(shard.weights)
	if step < 0 {
		durations = append(durations, added)
	} else {
		durations[step] += added
	}
	slices.SortFunc(durations, func(left, right int64) int { return cmp.Compare(right, left) })
	return scheduleGoSteps(durations)
}

func (shard *goShardBuilder) add(unit goShardUnit, step int, added int64) {
	if step >= 0 {
		shard.steps[step].Tests = append(shard.steps[step].Tests, unit.selection.Tests...)
		shard.weights[step] += added
		return
	}
	selection := unit.selection
	selection.Tests = slices.Clone(selection.Tests)
	shard.byPackage[selection.Packages[0]] = len(shard.steps)
	shard.steps = append(shard.steps, selection)
	shard.weights = append(shard.weights, added)
}

// scheduleGoSteps returns the makespan of steps taken in order by the runner's
// workers, each step going to the worker that becomes free first.
func scheduleGoSteps(durations []int64) int64 {
	workers := make([]int64, goRaceWorkers)
	for _, duration := range durations {
		workers[slices.Index(workers, slices.Min(workers))] += duration
	}
	return slices.Max(workers)
}

// predictGoShardMakespan prices a shard's steps with the weights and schedules
// them in plan order.
func predictGoShardMakespan(plan []Selection, weights goShardWeights) int64 {
	durations := make([]int64, 0, len(plan))
	for _, selection := range plan {
		if selection.Group == "go" && len(selection.Packages) == 1 {
			durations = append(durations, weights.selection(selection))
		}
	}
	return scheduleGoSteps(durations)
}
