package testinfra

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestSelectPlanReconcilesGoShardsExactlyOnce(t *testing.T) {
	selections, exclusions, err := fullGoSelections(testRepositoryRoot(t), "exactly-once shard contract")
	if err != nil {
		t.Fatal(err)
	}
	plan := Plan{Profile: ProfileFull, Selections: append(selections, Selection{Group: "runtime"}), Excluded: exclusions}
	for count := 1; count <= 6; count++ {
		var producers []string
		results := map[string]Result{}
		for index := 0; index < count; index++ {
			shard, err := SelectPlan(plan, []string{"go"}, index, count)
			if err != nil {
				t.Fatal(err)
			}
			again, err := SelectPlan(plan, []string{"go"}, index, count)
			if err != nil || !reflect.DeepEqual(shard, again) {
				t.Fatalf("planning shard %d of %d twice gave different plans (error %v)", index, count, err)
			}
			packages := map[string]bool{}
			for _, selection := range shard.Selections {
				if selection.Group != "go" || packages[selection.Packages[0]] {
					t.Fatalf("shard %d of %d holds %s twice or a non-Go selection", index, count, selection.Packages)
				}
				packages[selection.Packages[0]] = true
			}
			producer := fmt.Sprintf("shard-%d", index)
			producers = append(producers, producer)
			results[producer] = Result{Plan: shard}
		}
		if failures := goShardPartitionFailures(producers, results); len(failures) > 0 {
			t.Fatalf("%d shards: %v", count, failures)
		}
	}
}

func TestBalanceGoSelectionsPlacesUnitsByWeight(t *testing.T) {
	// p/s is sliceable: four 8 s tests and 2 s overhead per step. p/w runs
	// whole in 20 s.
	weights, err := parseGoShardWeights([]byte(`{"version": 2, "generated_at": "2026-10-11", "base": "b", "source_runs": ["1"], "cpu_models": {"Fixture CPU": {"factor": 1, "jobs": 1, "samples": 1, "shared_units": 0}}, "median_test_ms": 1,
		"sliceable": ["p/s"], "packages": {
			"p/s": {"elapsed_ms": 32000, "overhead_ms": 2000, "tests_ms": {"TestA": 8000, "TestB": 8000, "TestC": 8000, "TestD": 8000}},
			"p/w": {"elapsed_ms": 20000, "overhead_ms": 0}}}`))
	if err != nil {
		t.Fatal(err)
	}
	selections := []Selection{
		{Group: "go", Packages: []string{"p/s"}, Tests: []string{"TestA", "TestB", "TestC", "TestD"}},
		{Group: "go", Packages: []string{"p/w"}, Tests: []string{"TestW"}},
	}
	for _, test := range []struct {
		name      string
		shards    int
		steps     [][]string
		makespans []int64
	}{
		{
			// p/w goes first to shard 0; each 8 s test then joins whichever
			// shard ends earliest, paying p/s's overhead once per shard, and
			// each shard starts its longest step first.
			name: "two shards", shards: 2,
			steps:     [][]string{{"p/w TestW", "p/s TestC TestD"}, {"p/s TestA TestB"}},
			makespans: []int64{20000, 18000},
		},
		{name: "one shard keeps a sliceable package whole", shards: 1, steps: [][]string{{"p/s TestA TestB TestC TestD", "p/w TestW"}}, makespans: []int64{34000}},
	} {
		t.Run(test.name, func(t *testing.T) {
			shards := balanceGoSelections(selections, test.shards, weights)
			for index, shard := range shards {
				var steps []string
				for _, selection := range shard {
					steps = append(steps, strings.Join(append([]string{selection.Packages[0]}, selection.Tests...), " "))
				}
				if !slices.Equal(steps, test.steps[index]) {
					t.Fatalf("shard %d steps = %q; want %q", index, steps, test.steps[index])
				}
				if makespan := predictGoShardMakespan(shard, weights); makespan != test.makespans[index] {
					t.Fatalf("shard %d makespan = %d ms; want %d", index, makespan, test.makespans[index])
				}
			}
		})
	}
}

func TestSelectPlanRejectsUnknownGroupAndInvalidShard(t *testing.T) {
	plan := Plan{Selections: []Selection{{Group: "go", Packages: []string{"a"}}}}
	if _, err := SelectPlan(plan, []string{"missing"}, 0, 1); err == nil {
		t.Fatal("unknown group accepted")
	}
	if _, err := SelectPlan(plan, []string{"go"}, 2, 2); err == nil {
		t.Fatal("invalid shard accepted")
	}
}
