package testinfra

import "testing"

func TestSelectPlanReconcilesGoShardsExactlyOnce(t *testing.T) {
	plan := Plan{Profile: ProfileFull, Selections: []Selection{
		{Group: "go", Packages: []string{"b"}, Tests: []string{"TestB"}, Dependencies: []string{"postgresql"}},
		{Group: "runtime"},
		{Group: "go", Packages: []string{"a"}, Tests: []string{"TestA"}},
	}}
	seen := map[string]int{}
	for index := 0; index < 2; index++ {
		shard, err := SelectPlan(plan, []string{"go"}, index, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, selection := range shard.Selections {
			seen[selection.Packages[0]+"/"+selection.Tests[0]]++
		}
	}
	if seen["a/TestA"] != 1 || seen["b/TestB"] != 1 || len(seen) != 2 {
		t.Fatalf("shard selections = %#v; want exact single execution", seen)
	}
}

func TestSelectPlanSlicesDominantPackagesAcrossRaceShards(t *testing.T) {
	calibration, err := loadGoShardCalibration()
	if err != nil {
		t.Fatal(err)
	}
	bridge := "github.com/tetral-ai/tetral/services/bridge"
	composition := "github.com/tetral-ai/tetral/integration"
	tests := map[string]string{
		"TestPostgreSQLProviderFailuresSettleOneTurnAndLaterInputContinues":                 composition,
		"TestPostgreSQLRuntimeAbortCancelsJoinedBackgroundCommand":                          bridge,
		"TestSubagentRetainedAssistantAndTerminalToolResultColdResume":                      composition,
		"TestSubagentFirstMailCloseBeforeRequestStartCancelsExactCustody":                   composition,
		"TestSubagentFirstMailInterruptedCloseColdResumeAndLaterInputProductionComposition": composition,
		"TestPostgreSQLReviewerRunExitClosesWithExactDurableAuthority":                      composition,
		"TestPostgreSQLBridgeAPIStoreRunMemoryEnforcesDurableMemoryQuotas":                  bridge,
		"TestPostgreSQLBridgeAPIStoreRunMemorySkipsRefreshForValidationErrors":              bridge,
		"TestPostgreSQLThreadLoopRejectsOversizedSubagentPromptBeforeBridgeMutation":        bridge,
		"TestPostgreSQLBridgeAPIStoreRunMemoryRejectsInvalidInputs":                         bridge,
	}
	plan := Plan{Profile: ProfileFull}
	for _, owner := range []string{bridge, composition} {
		selection := Selection{Group: "go", Packages: []string{owner}}
		for test, packageName := range tests {
			if packageName == owner {
				if calibration.Packages[owner].TestsMS[test] <= 0 {
					t.Fatalf("moved case %s lost its calibrated weight under %s", test, owner)
				}
				selection.Tests = append(selection.Tests, test)
			}
		}
		plan.Selections = append(plan.Selections, selection)
	}
	seen := map[string]int{}
	weights := make([]int, 4)
	for shardIndex := 0; shardIndex < 4; shardIndex++ {
		shard, err := SelectPlan(plan, []string{"go"}, shardIndex, 4)
		if err != nil {
			t.Fatal(err)
		}
		packages := map[string]bool{}
		for _, selection := range shard.Selections {
			packageName := selection.Packages[0]
			if packages[packageName] {
				t.Fatalf("shard %d started multiple processes for %s", shardIndex, packageName)
			}
			packages[packageName] = true
			for _, test := range selection.Tests {
				if tests[test] != packageName {
					t.Fatalf("shard %d moved %s to wrong owner %s", shardIndex, test, packageName)
				}
				seen[test]++
				weights[shardIndex] += calibration.Packages[packageName].TestsMS[test]
			}
		}
	}
	for test := range tests {
		if seen[test] != 1 {
			t.Fatalf("%s executed %d times", test, seen[test])
		}
	}
	minWeight, maxWeight := weights[0], weights[0]
	for _, weight := range weights[1:] {
		minWeight = min(minWeight, weight)
		maxWeight = max(maxWeight, weight)
	}
	if maxWeight > minWeight*2 {
		t.Fatalf("calibrated shard weights are not bounded: %v", weights)
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
