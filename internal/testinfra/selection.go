package testinfra

import (
	"fmt"
	"sort"
	"strings"
)

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
		shards, err := balanceGoSelections(goSelections, shardCount)
		if err != nil {
			return Plan{}, err
		}
		selected = append(selected, shards[shardIndex]...)
	}
	if len(selected) == 0 {
		return Plan{}, fmt.Errorf("selected execution slice is empty")
	}
	plan.Selections = selected
	plan.Dependencies = selectedDependencies(selected)
	return plan, nil
}

func balanceGoSelections(selections []Selection, shardCount int) ([][]Selection, error) {
	type weightedSelection struct {
		selection Selection
		weight    int
	}
	calibration, err := loadGoShardCalibration()
	if err != nil {
		return nil, err
	}
	var ordered []weightedSelection
	for _, selection := range selections {
		packageName := ""
		if len(selection.Packages) == 1 {
			packageName = selection.Packages[0]
		}
		packageCalibration, sliced := calibration.Packages[packageName]
		if shardCount > 1 && sliced && len(selection.Tests) > 0 {
			for _, test := range selection.Tests {
				weight := packageCalibration.TestsMS[test]
				if weight == 0 {
					weight = packageCalibration.DefaultMS
				}
				part := selection
				part.Tests = []string{test}
				part.Reason = "duration-balanced top-level package slice"
				ordered = append(ordered, weightedSelection{selection: part, weight: weight})
			}
			continue
		}
		ordered = append(ordered, weightedSelection{selection: selection, weight: goSelectionWeight(selection) * 350})
	}
	sort.Slice(ordered, func(i, j int) bool {
		left, right := ordered[i].weight, ordered[j].weight
		if left != right {
			return left > right
		}
		leftName := ordered[i].selection.Packages[0] + "\x00" + strings.Join(ordered[i].selection.Tests, "\x00")
		rightName := ordered[j].selection.Packages[0] + "\x00" + strings.Join(ordered[j].selection.Tests, "\x00")
		return leftName < rightName
	})
	parts := make([][]weightedSelection, shardCount)
	weights := make([]int, shardCount)
	for _, item := range ordered {
		target := 0
		for index := 1; index < shardCount; index++ {
			if weights[index] < weights[target] {
				target = index
			}
		}
		parts[target] = append(parts[target], item)
		weights[target] += item.weight
	}
	shards := make([][]Selection, shardCount)
	for index := range shards {
		byPackage := map[string]int{}
		for _, item := range parts[index] {
			selection := item.selection
			packageName := selection.Packages[0]
			if existing, ok := byPackage[packageName]; ok {
				shards[index][existing].Tests = append(shards[index][existing].Tests, selection.Tests...)
				continue
			}
			byPackage[packageName] = len(shards[index])
			shards[index] = append(shards[index], selection)
		}
		sort.Slice(shards[index], func(i, j int) bool {
			return shards[index][i].Packages[0] < shards[index][j].Packages[0]
		})
		for selectionIndex := range shards[index] {
			sort.Strings(shards[index][selectionIndex].Tests)
		}
	}
	return shards, nil
}

func goSelectionWeight(selection Selection) int {
	weight := max(1, len(selection.Tests))
	if len(selection.Packages) == 0 {
		return weight
	}
	// These indivisible owners dominate current hosted-runner wall time. Values
	// are advisory relative weights; selection remains the reconciled inventory.
	switch selection.Packages[0] {
	case "github.com/tetral-ai/tetral/services/bridge":
		return weight + 1000
	case "github.com/tetral-ai/tetral/internal/httpapi":
		return weight + 700
	case "github.com/tetral-ai/tetral/internal/sandbox":
		return weight + 350
	case "github.com/tetral-ai/tetral/internal/storage", "github.com/tetral-ai/tetral/internal/queue":
		return weight + 200
	default:
		return weight
	}
}
