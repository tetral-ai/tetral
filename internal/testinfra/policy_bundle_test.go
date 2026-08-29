package testinfra

import (
	"encoding/json"
	"testing"
)

func TestGitHubPolicyBundleRehearsesEveryRollbackPoint(t *testing.T) {
	bundle, err := BuildGitHubPolicyBundle("tetral-ai/tetral", validPolicyPreState(t), validLegacyArchive())
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyGitHubPolicyBundle(bundle); err != nil {
		t.Fatal(err)
	}
	if len(bundle.Transitions) != 6 || bundle.Transitions[1].ID != "restore-legacy-ruleset" || len(bundle.Transitions[1].RequiredEvidence) == 0 {
		t.Fatalf("cutover ordering is incomplete: %+v", bundle.Transitions)
	}
	for failurePoint := 0; failurePoint <= len(bundle.Transitions); failurePoint++ {
		if err := RehearseGitHubPolicyRollback(bundle, failurePoint); err != nil {
			t.Fatalf("rollback after transition %d: %v", failurePoint, err)
		}
	}
	if err := RehearseFinalStateRecovery(bundle); err != nil {
		t.Fatal(err)
	}
	if got := recoveryStepIDs(bundle.FinalStateRecovery.Normal); !sameStringsInOrder(got, []string{
		"normal-open-restore-pr", "normal-verify-restore-head", "normal-merge-restore-pr", "normal-restore-legacy-ruleset",
	}) {
		t.Fatalf("normal recovery order = %v", got)
	}
	if got := recoveryStepIDs(bundle.FinalStateRecovery.GateUnavailable); !sameStringsInOrder(got, []string{
		"emergency-begin-exclusive-window", "emergency-remove-merge-gate", "emergency-merge-restore-head",
		"emergency-verify-legacy-contexts", "emergency-restore-legacy-ruleset", "emergency-end-exclusive-window",
	}) {
		t.Fatalf("Gate-unavailable recovery order = %v", got)
	}
}

func TestGitHubPolicyBundleRejectsDriftAndWrongRequiredSource(t *testing.T) {
	pre := validPolicyPreState(t)
	bundle, err := BuildGitHubPolicyBundle("tetral-ai/tetral", pre, validLegacyArchive())
	if err != nil {
		t.Fatal(err)
	}
	bundle.Transitions[0].ExpectedPreHash = "sha256:wrong"
	if err := VerifyGitHubPolicyBundle(bundle); err == nil {
		t.Fatal("pre-state drift passed")
	}
	pre = validPolicyPreState(t)
	checks, err := requiredChecks(pre.Ruleset)
	if err != nil || len(checks) == 0 {
		t.Fatal(err)
	}
	for _, rule := range pre.Ruleset.Rules {
		if rule["type"] == "required_status_checks" {
			rows := rule["parameters"].(map[string]any)["required_status_checks"].([]any)
			rows[0].(map[string]any)["integration_id"] = float64(999)
		}
	}
	if _, err := BuildGitHubPolicyBundle("tetral-ai/tetral", pre, validLegacyArchive()); err == nil {
		t.Fatal("required check from the wrong GitHub App passed")
	}
}

func TestGitHubPolicyBundleRejectsUnsafeRecoveryOrdering(t *testing.T) {
	bundle, err := BuildGitHubPolicyBundle("tetral-ai/tetral", validPolicyPreState(t), validLegacyArchive())
	if err != nil {
		t.Fatal(err)
	}
	steps := bundle.FinalStateRecovery.GateUnavailable
	steps[2], steps[3] = steps[3], steps[2]
	bundle.Digest, err = policyBundleDigest(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyGitHubPolicyBundle(bundle); err == nil {
		t.Fatal("legacy contexts were allowed to verify before the restore workflow was merged")
	}
}

func TestGitHubPolicyBundleRequiresCompleteRulesetFoundation(t *testing.T) {
	for _, removed := range []string{"deletion", "non_fast_forward", "pull_request"} {
		pre := validPolicyPreState(t)
		filtered := make([]map[string]any, 0, len(pre.Ruleset.Rules)-1)
		for _, rule := range pre.Ruleset.Rules {
			if rule["type"] != removed {
				filtered = append(filtered, rule)
			}
		}
		pre.Ruleset.Rules = filtered
		if _, err := BuildGitHubPolicyBundle("tetral-ai/tetral", pre, validLegacyArchive()); err == nil {
			t.Fatalf("ruleset without %s protection passed", removed)
		}
	}
}

func validPolicyPreState(t *testing.T) PolicyState {
	t.Helper()
	var ruleset GitHubRuleset
	body := `{
  "id": 19970716,
  "name": "main",
  "target": "branch",
  "enforcement": "active",
  "bypass_actors": [{"actor_id": null, "actor_type": "OrganizationAdmin", "bypass_mode": "always"}],
  "conditions": {"ref_name": {"exclude": [], "include": ["~DEFAULT_BRANCH"]}},
  "rules": [
    {"type": "deletion"},
    {"type": "non_fast_forward"},
    {"type": "pull_request", "parameters": {"required_approving_review_count": 0, "required_review_thread_resolution": false, "allowed_merge_methods": ["merge", "squash", "rebase"]}},
    {"type": "required_status_checks", "parameters": {"strict_required_status_checks_policy": true, "do_not_enforce_on_create": false, "required_status_checks": []}}
  ]
}`
	if err := json.Unmarshal([]byte(body), &ruleset); err != nil {
		t.Fatal(err)
	}
	for _, rule := range ruleset.Rules {
		if rule["type"] != "required_status_checks" {
			continue
		}
		rows := []any{}
		for _, context := range legacyRequiredChecks {
			rows = append(rows, map[string]any{"context": context, "integration_id": float64(githubActionsAppID)})
		}
		rule["parameters"].(map[string]any)["required_status_checks"] = rows
	}
	return PolicyState{
		Ruleset:    ruleset,
		Repository: RepositoryMergePolicy{AllowMergeCommit: true, AllowSquashMerge: true, AllowRebaseMerge: true, AllowAutoMerge: false},
		Actions:    RepositoryActionsPolicy{Enabled: true, AllowedActions: "all", SHAPinningRequired: false},
	}
}

func validLegacyArchive() LegacyWorkflowArchive {
	return LegacyWorkflowArchive{SourceCommit: "commit", TreeSHA: "tree", Path: ".github/workflows/engine-ci.yml", BlobSHA: "blob", ArchiveSHA: "sha256:archive"}
}

func recoveryStepIDs(steps []PolicyRecoveryStep) []string {
	result := make([]string, 0, len(steps))
	for _, step := range steps {
		result = append(result, step.ID)
	}
	return result
}

func sameStringsInOrder(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
