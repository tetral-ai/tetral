package testinfra

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

const githubPolicyBundleSchema = "tetral.github-policy-cutover/v1"

var legacyRequiredChecks = []string{
	"agent-runtime-ts",
	"gateway-ts",
	"go-static",
	"go-test (0)",
	"go-test (1)",
	"go-test (2)",
	"go-test (3)",
	"helm-chart",
	"k8s-manifests",
	"sandbox-local-image-smoke",
	"security",
}

func LegacyRequiredChecks() []string {
	return append([]string(nil), legacyRequiredChecks...)
}

type GitHubRuleset struct {
	ID           int64            `json:"id,omitempty"`
	Name         string           `json:"name"`
	Target       string           `json:"target"`
	Enforcement  string           `json:"enforcement"`
	BypassActors []map[string]any `json:"bypass_actors"`
	Conditions   map[string]any   `json:"conditions"`
	Rules        []map[string]any `json:"rules"`
}

type RepositoryMergePolicy struct {
	AllowMergeCommit bool `json:"allow_merge_commit"`
	AllowSquashMerge bool `json:"allow_squash_merge"`
	AllowRebaseMerge bool `json:"allow_rebase_merge"`
	AllowAutoMerge   bool `json:"allow_auto_merge"`
}

type RepositoryActionsPolicy struct {
	Enabled            bool   `json:"enabled"`
	AllowedActions     string `json:"allowed_actions"`
	SHAPinningRequired bool   `json:"sha_pinning_required"`
}

type LegacyWorkflowArchive struct {
	SourceCommit     string   `json:"source_commit"`
	TreeSHA          string   `json:"tree_sha"`
	Path             string   `json:"workflow_path"`
	BlobSHA          string   `json:"workflow_blob_sha"`
	ArchiveSHA       string   `json:"archive_sha256"`
	ProofResultSHA   string   `json:"proof_result_sha256"`
	ProofCommand     []string `json:"proof_command"`
	RequiredContexts []string `json:"required_contexts"`
}

type PolicyState struct {
	Ruleset    GitHubRuleset           `json:"ruleset"`
	Repository RepositoryMergePolicy   `json:"repository"`
	Actions    RepositoryActionsPolicy `json:"actions"`
}

type PolicyMutation struct {
	ID                 string          `json:"id"`
	Surface            string          `json:"surface"`
	Method             string          `json:"method"`
	Endpoint           string          `json:"endpoint"`
	ExpectedPreHash    string          `json:"expected_pre_state_sha256"`
	Payload            json.RawMessage `json:"payload"`
	ExpectedReadback   string          `json:"expected_readback_sha256"`
	ReadbackProjection string          `json:"readback_projection"`
	RequiredEvidence   []string        `json:"required_evidence,omitempty"`
	Compensation       json.RawMessage `json:"compensation_payload"`
	CompensationHash   string          `json:"compensation_readback_sha256"`
}

type PolicyRecoveryStep struct {
	ID                 string          `json:"id"`
	Action             string          `json:"action"`
	Surface            string          `json:"surface,omitempty"`
	Method             string          `json:"method,omitempty"`
	Endpoint           string          `json:"endpoint,omitempty"`
	ExpectedPreHash    string          `json:"expected_pre_state_sha256,omitempty"`
	Payload            json.RawMessage `json:"payload,omitempty"`
	ExpectedReadback   string          `json:"expected_readback_sha256,omitempty"`
	ReadbackProjection string          `json:"readback_projection,omitempty"`
	RequiredEvidence   []string        `json:"required_evidence"`
	CompensationAction string          `json:"compensation_action,omitempty"`
	Compensation       json.RawMessage `json:"compensation_payload,omitempty"`
	CompensationHash   string          `json:"compensation_readback_sha256,omitempty"`
}

type FinalStateRecovery struct {
	Normal          []PolicyRecoveryStep `json:"normal"`
	GateUnavailable []PolicyRecoveryStep `json:"gate_unavailable"`
}

type GitHubPolicyBundle struct {
	Schema             string                `json:"schema"`
	Repository         string                `json:"repository"`
	RulesetID          int64                 `json:"ruleset_id"`
	PreState           PolicyState           `json:"pre_state"`
	FinalState         PolicyState           `json:"final_state"`
	Archive            LegacyWorkflowArchive `json:"legacy_workflow_archive"`
	Transitions        []PolicyMutation      `json:"transitions"`
	FinalStateRecovery FinalStateRecovery    `json:"final_state_recovery"`
	Digest             string                `json:"bundle_sha256"`
}

func BuildGitHubPolicyBundle(repository string, pre PolicyState, archive LegacyWorkflowArchive) (GitHubPolicyBundle, error) {
	if repository == "" || pre.Ruleset.ID <= 0 || pre.Ruleset.Name == "" || pre.Ruleset.Target != "branch" || pre.Ruleset.Enforcement != "active" {
		return GitHubPolicyBundle{}, fmt.Errorf("repository or active branch ruleset pre-state is incomplete")
	}
	if archive.SourceCommit == "" || archive.TreeSHA == "" || archive.Path != ".github/workflows/engine-ci.yml" || archive.BlobSHA == "" || archive.ArchiveSHA == "" ||
		archive.ProofResultSHA == "" || !sameOrderedStrings(archive.ProofCommand, []string{"make", "test-full"}) || !sameStrings(archive.RequiredContexts, legacyRequiredChecks) {
		return GitHubPolicyBundle{}, fmt.Errorf("legacy workflow archive identity is incomplete")
	}
	if err := verifyLegacyRequiredChecks(pre.Ruleset); err != nil {
		return GitHubPolicyBundle{}, err
	}
	if err := verifyRulesetFoundation(pre.Ruleset); err != nil {
		return GitHubPolicyBundle{}, err
	}
	if !pre.Repository.AllowMergeCommit || !pre.Repository.AllowSquashMerge || !pre.Repository.AllowRebaseMerge || pre.Repository.AllowAutoMerge {
		return GitHubPolicyBundle{}, fmt.Errorf("repository merge pre-state does not match the recorded legacy policy")
	}
	rulesetID := pre.Ruleset.ID
	pre.Ruleset.ID = 0
	intermediate := cloneRuleset(pre.Ruleset)
	if err := replaceRequiredChecks(&intermediate, append(append([]string(nil), legacyRequiredChecks...), "Merge Gate")); err != nil {
		return GitHubPolicyBundle{}, err
	}
	finalRuleset := cloneRuleset(intermediate)
	finalRuleset.BypassActors = []map[string]any{}
	if err := replaceRequiredChecks(&finalRuleset, []string{"Merge Gate"}); err != nil {
		return GitHubPolicyBundle{}, err
	}
	if err := updatePullRequestRule(&finalRuleset); err != nil {
		return GitHubPolicyBundle{}, err
	}
	final := PolicyState{
		Ruleset:    finalRuleset,
		Repository: RepositoryMergePolicy{AllowMergeCommit: true, AllowSquashMerge: true, AllowRebaseMerge: false, AllowAutoMerge: false},
		Actions:    RepositoryActionsPolicy{Enabled: pre.Actions.Enabled, AllowedActions: pre.Actions.AllowedActions, SHAPinningRequired: true},
	}
	if !final.Actions.Enabled || final.Actions.AllowedActions == "" {
		return GitHubPolicyBundle{}, fmt.Errorf("Actions pre-state is disabled or incomplete")
	}

	rulesetEndpoint := fmt.Sprintf("repos/%s/rulesets/%d", repository, rulesetID)
	repositoryEndpoint := "repos/" + repository
	actionsEndpoint := repositoryEndpoint + "/actions/permissions"
	transitions := []PolicyMutation{}
	appendMutation := func(id, surface, method, endpoint string, before, after, compensation any, evidence ...string) error {
		beforeHash, err := canonicalHash(before)
		if err != nil {
			return err
		}
		afterBody, err := canonicalJSON(after)
		if err != nil {
			return err
		}
		afterHash, err := canonicalHash(after)
		if err != nil {
			return err
		}
		compensationBody, err := canonicalJSON(compensation)
		if err != nil {
			return err
		}
		compensationHash, err := canonicalHash(compensation)
		if err != nil {
			return err
		}
		transitions = append(transitions, PolicyMutation{ID: id, Surface: surface, Method: method, Endpoint: endpoint,
			ExpectedPreHash: beforeHash, Payload: afterBody, ExpectedReadback: afterHash,
			ReadbackProjection: policyReadbackProjection(surface), RequiredEvidence: evidence,
			Compensation: compensationBody, CompensationHash: compensationHash})
		return nil
	}
	if err := appendMutation("add-merge-gate", "ruleset", "PUT", rulesetEndpoint, pre.Ruleset, intermediate, pre.Ruleset); err != nil {
		return GitHubPolicyBundle{}, err
	}
	if err := appendMutation("restore-legacy-ruleset", "ruleset", "PUT", rulesetEndpoint, intermediate, pre.Ruleset, intermediate, "all twelve exact-head checks reported"); err != nil {
		return GitHubPolicyBundle{}, err
	}
	if err := appendMutation("reapply-merge-gate", "ruleset", "PUT", rulesetEndpoint, pre.Ruleset, intermediate, pre.Ruleset, "legacy verdict usable after restore"); err != nil {
		return GitHubPolicyBundle{}, err
	}
	if err := appendMutation("finalize-merge-gate", "ruleset", "PUT", rulesetEndpoint, intermediate, final.Ruleset, intermediate, "all twelve exact-head checks reported after reapply"); err != nil {
		return GitHubPolicyBundle{}, err
	}
	if err := appendMutation("disable-rebase", "repository", "PATCH", repositoryEndpoint, pre.Repository, final.Repository, pre.Repository, "Merge Gate is the sole required exact-head check"); err != nil {
		return GitHubPolicyBundle{}, err
	}
	if err := appendMutation("require-action-shas", "actions", "PUT", actionsEndpoint, pre.Actions, final.Actions, pre.Actions, "Action inventory matches every executable reference"); err != nil {
		return GitHubPolicyBundle{}, err
	}
	recovery, err := buildFinalStateRecovery(repository, rulesetEndpoint, pre, final)
	if err != nil {
		return GitHubPolicyBundle{}, err
	}
	bundle := GitHubPolicyBundle{Schema: githubPolicyBundleSchema, Repository: repository, RulesetID: rulesetID,
		PreState: pre, FinalState: final, Archive: archive, Transitions: transitions, FinalStateRecovery: recovery}
	digest, err := policyBundleDigest(bundle)
	if err != nil {
		return GitHubPolicyBundle{}, err
	}
	bundle.Digest = digest
	return bundle, nil
}

func VerifyGitHubPolicyBundle(bundle GitHubPolicyBundle) error {
	if bundle.Schema != githubPolicyBundleSchema || bundle.Repository == "" || bundle.RulesetID <= 0 || bundle.PreState.Ruleset.ID != 0 {
		return fmt.Errorf("policy bundle identity is incomplete")
	}
	digest, err := policyBundleDigest(bundle)
	if err != nil || digest != bundle.Digest {
		return fmt.Errorf("policy bundle digest mismatch")
	}
	current := map[string]any{"ruleset": bundle.PreState.Ruleset, "repository": bundle.PreState.Repository, "actions": bundle.PreState.Actions}
	for _, transition := range bundle.Transitions {
		before, exists := current[transition.Surface]
		if !exists {
			return fmt.Errorf("transition %q has unknown surface", transition.ID)
		}
		if transition.ReadbackProjection != policyReadbackProjection(transition.Surface) {
			return fmt.Errorf("transition %q has the wrong readback projection", transition.ID)
		}
		hash, err := canonicalHash(before)
		if err != nil || hash != transition.ExpectedPreHash {
			return fmt.Errorf("transition %q pre-state mismatch", transition.ID)
		}
		var next any
		if err := json.Unmarshal(transition.Payload, &next); err != nil {
			return fmt.Errorf("transition %q payload: %w", transition.ID, err)
		}
		hash, err = canonicalHash(next)
		if err != nil || hash != transition.ExpectedReadback {
			return fmt.Errorf("transition %q readback mismatch", transition.ID)
		}
		var compensation any
		if err := json.Unmarshal(transition.Compensation, &compensation); err != nil {
			return fmt.Errorf("transition %q compensation: %w", transition.ID, err)
		}
		hash, err = canonicalHash(compensation)
		if err != nil || hash != transition.CompensationHash || hash != transition.ExpectedPreHash {
			return fmt.Errorf("transition %q compensation does not restore its pre-state", transition.ID)
		}
		current[transition.Surface] = next
	}
	for surface, final := range map[string]any{"ruleset": bundle.FinalState.Ruleset, "repository": bundle.FinalState.Repository, "actions": bundle.FinalState.Actions} {
		got, err := canonicalHash(current[surface])
		want, wantErr := canonicalHash(final)
		if err != nil || wantErr != nil || got != want {
			return fmt.Errorf("policy bundle final %s state mismatch", surface)
		}
	}
	if err := verifyFinalStateRecovery(bundle); err != nil {
		return err
	}
	return nil
}

func RehearseGitHubPolicyRollback(bundle GitHubPolicyBundle, failAfter int) error {
	if err := VerifyGitHubPolicyBundle(bundle); err != nil {
		return err
	}
	if failAfter < 0 || failAfter > len(bundle.Transitions) {
		return fmt.Errorf("invalid rollback failure point")
	}
	current := map[string]any{"ruleset": bundle.PreState.Ruleset, "repository": bundle.PreState.Repository, "actions": bundle.PreState.Actions}
	completed := bundle.Transitions[:failAfter]
	for _, transition := range completed {
		var next any
		if err := json.Unmarshal(transition.Payload, &next); err != nil {
			return err
		}
		current[transition.Surface] = next
	}
	for index := len(completed) - 1; index >= 0; index-- {
		transition := completed[index]
		var compensation any
		if err := json.Unmarshal(transition.Compensation, &compensation); err != nil {
			return err
		}
		current[transition.Surface] = compensation
	}
	for surface, pre := range map[string]any{"ruleset": bundle.PreState.Ruleset, "repository": bundle.PreState.Repository, "actions": bundle.PreState.Actions} {
		got, err := canonicalHash(current[surface])
		want, wantErr := canonicalHash(pre)
		if err != nil || wantErr != nil || got != want {
			return fmt.Errorf("rollback at transition %d did not restore %s", failAfter, surface)
		}
	}
	return nil
}

func RehearseFinalStateRecovery(bundle GitHubPolicyBundle) error {
	if err := VerifyGitHubPolicyBundle(bundle); err != nil {
		return err
	}
	for name, steps := range map[string][]PolicyRecoveryStep{
		"normal":           bundle.FinalStateRecovery.Normal,
		"gate-unavailable": bundle.FinalStateRecovery.GateUnavailable,
	} {
		if err := rehearseRecoveryCompletion(bundle, name, steps, defaultRecoveryFixture(name)); err != nil {
			return err
		}
		for failurePoint := 0; failurePoint <= len(steps); failurePoint++ {
			if err := rehearseRecoveryFailure(bundle, name, steps, failurePoint, defaultRecoveryFixture(name)); err != nil {
				return err
			}
		}
	}
	return nil
}

type recoveryFixture struct {
	Ruleset           any
	GateAvailable     bool
	WorkflowPresent   bool
	RestorePROpen     bool
	Exclusive         bool
	AutoMerge         bool
	OtherMergeablePRs int
	AuthorizedHead    string
	MergedHead        string
	UnexpectedMerge   bool
	LegacyContexts    int
}

func defaultRecoveryFixture(name string) recoveryFixture {
	return recoveryFixture{GateAvailable: name == "normal", AuthorizedHead: "restore-head"}
}

func rehearseRecoveryCompletion(bundle GitHubPolicyBundle, name string, steps []PolicyRecoveryStep, fixture recoveryFixture) error {
	fixture.Ruleset = bundle.FinalState.Ruleset
	for _, step := range steps {
		if err := applyRecoveryStep(&fixture, step); err != nil {
			return fmt.Errorf("%s recovery step %q: %w", name, step.ID, err)
		}
	}
	got, err := canonicalHash(fixture.Ruleset)
	want, wantErr := canonicalHash(bundle.PreState.Ruleset)
	if err != nil || wantErr != nil || got != want || !fixture.WorkflowPresent || fixture.Exclusive || fixture.RestorePROpen || fixture.LegacyContexts != len(legacyRequiredChecks) {
		return fmt.Errorf("%s recovery did not reach the complete legacy state", name)
	}
	return nil
}

func rehearseRecoveryFailure(bundle GitHubPolicyBundle, name string, steps []PolicyRecoveryStep, failAfter int, fixture recoveryFixture) error {
	fixture.Ruleset = bundle.FinalState.Ruleset
	completed := steps[:failAfter]
	for _, step := range completed {
		if err := applyRecoveryStep(&fixture, step); err != nil {
			return fmt.Errorf("%s failure rehearsal step %q: %w", name, step.ID, err)
		}
	}
	for index := len(completed) - 1; index >= 0; index-- {
		if err := compensateRecoveryStep(&fixture, completed[index]); err != nil {
			return fmt.Errorf("%s failure compensation %q: %w", name, completed[index].ID, err)
		}
	}
	want, _ := canonicalHash(bundle.FinalState.Ruleset)
	got, err := canonicalHash(fixture.Ruleset)
	if err != nil || got != want || fixture.Exclusive || fixture.RestorePROpen {
		return fmt.Errorf("%s failure point %d did not return to a safe Gate-protected state", name, failAfter)
	}
	return nil
}

func applyRecoveryStep(fixture *recoveryFixture, step PolicyRecoveryStep) error {
	switch step.Action {
	case "open-exact-restore-pr":
		if fixture.RestorePROpen || fixture.AuthorizedHead == "" {
			return fmt.Errorf("restore PR identity is unavailable")
		}
		fixture.RestorePROpen = true
	case "verify-restore-head":
		if !fixture.RestorePROpen || !fixture.GateAvailable {
			return fmt.Errorf("restore head lacks an available Merge Gate")
		}
		fixture.LegacyContexts = len(legacyRequiredChecks)
	case "begin-exclusive-maintenance":
		if fixture.AutoMerge || fixture.OtherMergeablePRs != 0 || fixture.UnexpectedMerge || fixture.AuthorizedHead == "" {
			return fmt.Errorf("exclusive maintenance preconditions are false")
		}
		fixture.Exclusive = true
	case "merge-exact-restore-head":
		if fixture.MergedHead != "" || fixture.UnexpectedMerge || !fixture.RestorePROpen && !fixture.Exclusive {
			return fmt.Errorf("restore merge is not exclusive")
		}
		if fixture.RestorePROpen && !fixture.GateAvailable {
			return fmt.Errorf("normal restore merge lacks Merge Gate")
		}
		fixture.MergedHead = fixture.AuthorizedHead
		fixture.WorkflowPresent = true
		fixture.RestorePROpen = false
	case "verify-legacy-contexts":
		if !fixture.WorkflowPresent || !fixture.Exclusive || fixture.UnexpectedMerge {
			return fmt.Errorf("legacy contexts cannot be attributed to the restore")
		}
		fixture.LegacyContexts = len(legacyRequiredChecks)
	case "update-ruleset":
		hash, err := canonicalHash(fixture.Ruleset)
		if err != nil || hash != step.ExpectedPreHash {
			return fmt.Errorf("ruleset pre-state mismatch")
		}
		if strings.Contains(step.ID, "restore-legacy") && (!fixture.WorkflowPresent || fixture.LegacyContexts != len(legacyRequiredChecks)) {
			return fmt.Errorf("legacy ruleset restored before its workflow can emit every context")
		}
		var next any
		if err := json.Unmarshal(step.Payload, &next); err != nil {
			return err
		}
		hash, err = canonicalHash(next)
		if err != nil || hash != step.ExpectedReadback {
			return fmt.Errorf("ruleset readback mismatch")
		}
		fixture.Ruleset = next
	case "end-exclusive-maintenance":
		if !fixture.Exclusive || fixture.UnexpectedMerge {
			return fmt.Errorf("exclusive maintenance state drifted")
		}
		fixture.Exclusive = false
	default:
		return fmt.Errorf("unknown recovery action %q", step.Action)
	}
	return nil
}

func compensateRecoveryStep(fixture *recoveryFixture, step PolicyRecoveryStep) error {
	switch step.CompensationAction {
	case "":
		return nil
	case "close-restore-pr":
		fixture.RestorePROpen = false
	case "end-exclusive-maintenance":
		fixture.Exclusive = false
	case "update-ruleset":
		var restored any
		if err := json.Unmarshal(step.Compensation, &restored); err != nil {
			return err
		}
		hash, err := canonicalHash(restored)
		if err != nil || hash != step.CompensationHash {
			return fmt.Errorf("ruleset compensation mismatch")
		}
		fixture.Ruleset = restored
	default:
		return fmt.Errorf("unknown compensation action %q", step.CompensationAction)
	}
	return nil
}

func buildFinalStateRecovery(repository, rulesetEndpoint string, pre, final PolicyState) (FinalStateRecovery, error) {
	updateStep := func(id string, before, after, compensation GitHubRuleset, evidence ...string) (PolicyRecoveryStep, error) {
		preHash, err := canonicalHash(before)
		if err != nil {
			return PolicyRecoveryStep{}, err
		}
		payload, err := canonicalJSON(after)
		if err != nil {
			return PolicyRecoveryStep{}, err
		}
		readbackHash, err := canonicalHash(after)
		if err != nil {
			return PolicyRecoveryStep{}, err
		}
		compensationBody, err := canonicalJSON(compensation)
		if err != nil {
			return PolicyRecoveryStep{}, err
		}
		compensationHash, err := canonicalHash(compensation)
		if err != nil {
			return PolicyRecoveryStep{}, err
		}
		return PolicyRecoveryStep{
			ID: id, Action: "update-ruleset", Surface: "ruleset", Method: "PUT", Endpoint: rulesetEndpoint,
			ExpectedPreHash: preHash, Payload: payload, ExpectedReadback: readbackHash, ReadbackProjection: policyReadbackProjection("ruleset"),
			RequiredEvidence: evidence, CompensationAction: "update-ruleset", Compensation: compensationBody, CompensationHash: compensationHash,
		}, nil
	}
	step := func(id, action string, evidence ...string) PolicyRecoveryStep {
		return PolicyRecoveryStep{ID: id, Action: action, RequiredEvidence: evidence}
	}

	normalRestore, err := updateStep("normal-restore-legacy-ruleset", final.Ruleset, pre.Ruleset, final.Ruleset,
		"restore commit is merged on main", "all eleven legacy contexts report on the restored workflow")
	if err != nil {
		return FinalStateRecovery{}, err
	}
	normal := []PolicyRecoveryStep{
		step("normal-open-restore-pr", "open-exact-restore-pr",
			"restore source equals the archived commit and tree", "only the legacy workflow restoration is proposed"),
		step("normal-verify-restore-head", "verify-restore-head",
			"Merge Gate succeeds on the exact restore head", "all eleven legacy contexts report on the exact restore head"),
		step("normal-merge-restore-pr", "merge-exact-restore-head",
			"reviewed restore head is unchanged", "Merge Gate is successful on that exact head"),
		normalRestore,
	}
	normal[0].CompensationAction = "close-restore-pr"

	temporary := cloneRuleset(final.Ruleset)
	if err := removeRequiredChecks(&temporary); err != nil {
		return FinalStateRecovery{}, err
	}
	removeGate, err := updateStep("emergency-remove-merge-gate", final.Ruleset, temporary, final.Ruleset,
		"separate Owner authorization is recorded", "exclusive maintenance preconditions remain true")
	if err != nil {
		return FinalStateRecovery{}, err
	}
	restoreLegacy, err := updateStep("emergency-restore-legacy-ruleset", temporary, pre.Ruleset, final.Ruleset,
		"restore commit is merged on main", "all eleven legacy contexts report on the restored workflow")
	if err != nil {
		return FinalStateRecovery{}, err
	}
	emergency := []PolicyRecoveryStep{
		step("emergency-begin-exclusive-window", "begin-exclusive-maintenance",
			"automatic merge is disabled", "all open pull requests and mergeability are captured",
			"no other pull request is mergeable", "exact reviewed restore head is recorded",
			"separate Owner authorization names repository "+repository),
		removeGate,
		step("emergency-merge-restore-head", "merge-exact-restore-head",
			"only the recorded restore head is merged", "no unexpected repository or settings change occurred"),
		step("emergency-verify-legacy-contexts", "verify-legacy-contexts",
			"all eleven legacy contexts report on the restored workflow", "no other merge occurred"),
		restoreLegacy,
		step("emergency-end-exclusive-window", "end-exclusive-maintenance",
			"legacy ruleset readback matches the captured pre-state", "temporary no-Gate payload is absent",
			"no unexpected merge or settings drift occurred"),
	}
	emergency[0].CompensationAction = "end-exclusive-maintenance"
	return FinalStateRecovery{Normal: normal, GateUnavailable: emergency}, nil
}

func verifyFinalStateRecovery(bundle GitHubPolicyBundle) error {
	expected, err := buildFinalStateRecovery(bundle.Repository,
		fmt.Sprintf("repos/%s/rulesets/%d", bundle.Repository, bundle.RulesetID), bundle.PreState, bundle.FinalState)
	if err != nil {
		return err
	}
	want, err := canonicalHash(expected)
	if err != nil {
		return err
	}
	got, err := canonicalHash(bundle.FinalStateRecovery)
	if err != nil || got != want {
		return fmt.Errorf("final-state recovery ordering or payload mismatch")
	}
	if bundle.FinalState.Repository.AllowAutoMerge {
		return fmt.Errorf("final-state recovery requires automatic merge to remain disabled")
	}
	return nil
}

func ReadPolicyPreState(rulesetPath, repositoryPath, actionsPath string) (PolicyState, error) {
	var state PolicyState
	for path, target := range map[string]any{rulesetPath: &state.Ruleset, repositoryPath: &state.Repository, actionsPath: &state.Actions} {
		body, err := os.ReadFile(path) //nolint:gosec // explicit operator-supplied read-only capture.
		if err != nil {
			return PolicyState{}, err
		}
		if err := json.Unmarshal(body, target); err != nil {
			return PolicyState{}, fmt.Errorf("decode policy capture %s: %w", path, err)
		}
	}
	return state, nil
}

func VerifyPolicyMutationReadback(mutation PolicyMutation, body []byte) error {
	value, err := normalizePolicyReadback(mutation.Surface, body)
	if err != nil {
		return err
	}
	if mutation.ReadbackProjection != policyReadbackProjection(mutation.Surface) {
		return fmt.Errorf("policy mutation uses the wrong readback projection")
	}
	hash, err := canonicalHash(value)
	if err != nil || hash != mutation.ExpectedReadback {
		return fmt.Errorf("policy mutation readback does not match the expected state")
	}
	return nil
}

func policyReadbackProjection(surface string) string {
	return "tetral.github-" + surface + "-readback/v1"
}

func normalizePolicyReadback(surface string, body []byte) (any, error) {
	switch surface {
	case "ruleset":
		var value GitHubRuleset
		if err := json.Unmarshal(body, &value); err != nil {
			return nil, fmt.Errorf("decode ruleset readback: %w", err)
		}
		value.ID = 0
		return value, nil
	case "repository":
		var value RepositoryMergePolicy
		if err := json.Unmarshal(body, &value); err != nil {
			return nil, fmt.Errorf("decode repository readback: %w", err)
		}
		return value, nil
	case "actions":
		var value RepositoryActionsPolicy
		if err := json.Unmarshal(body, &value); err != nil {
			return nil, fmt.Errorf("decode Actions readback: %w", err)
		}
		return value, nil
	default:
		return nil, fmt.Errorf("unknown policy readback surface %q", surface)
	}
}

func verifyLegacyRequiredChecks(ruleset GitHubRuleset) error {
	checks, err := requiredChecks(ruleset)
	if err != nil {
		return err
	}
	if !sameStrings(checks, legacyRequiredChecks) {
		return fmt.Errorf("ruleset required checks do not match the legacy authority")
	}
	return nil
}

func verifyRulesetFoundation(ruleset GitHubRuleset) error {
	counts := map[string]int{}
	for _, rule := range ruleset.Rules {
		kind, ok := rule["type"].(string)
		if !ok || kind == "" {
			return fmt.Errorf("ruleset contains a rule without a type")
		}
		counts[kind]++
	}
	for _, kind := range []string{"deletion", "non_fast_forward", "pull_request", "required_status_checks"} {
		if counts[kind] != 1 {
			return fmt.Errorf("ruleset must contain exactly one %s rule", kind)
		}
	}
	refName, ok := ruleset.Conditions["ref_name"].(map[string]any)
	if !ok {
		return fmt.Errorf("ruleset default-branch condition is missing")
	}
	include, ok := refName["include"].([]any)
	if !ok || len(include) != 1 || include[0] != "~DEFAULT_BRANCH" {
		return fmt.Errorf("ruleset does not target exactly the default branch")
	}
	exclude, ok := refName["exclude"].([]any)
	if !ok || len(exclude) != 0 {
		return fmt.Errorf("ruleset default-branch exclusions are unexpected")
	}
	return nil
}

func requiredChecks(ruleset GitHubRuleset) ([]string, error) {
	for _, rule := range ruleset.Rules {
		if rule["type"] != "required_status_checks" {
			continue
		}
		parameters, ok := rule["parameters"].(map[string]any)
		if !ok || parameters["strict_required_status_checks_policy"] != true {
			return nil, fmt.Errorf("required status checks are not strict")
		}
		rows, ok := parameters["required_status_checks"].([]any)
		if !ok {
			return nil, fmt.Errorf("required status checks are malformed")
		}
		checks := make([]string, 0, len(rows))
		for _, row := range rows {
			item, ok := row.(map[string]any)
			if !ok || item["integration_id"] != float64(githubActionsAppID) {
				return nil, fmt.Errorf("required status check has the wrong source application")
			}
			context, ok := item["context"].(string)
			if !ok || context == "" {
				return nil, fmt.Errorf("required status check context is malformed")
			}
			checks = append(checks, context)
		}
		return checks, nil
	}
	return nil, fmt.Errorf("ruleset has no required status checks")
}

func replaceRequiredChecks(ruleset *GitHubRuleset, contexts []string) error {
	sort.Strings(contexts)
	for _, rule := range ruleset.Rules {
		if rule["type"] != "required_status_checks" {
			continue
		}
		parameters, ok := rule["parameters"].(map[string]any)
		if !ok {
			return fmt.Errorf("required status checks are malformed")
		}
		rows := make([]any, 0, len(contexts))
		for _, context := range contexts {
			rows = append(rows, map[string]any{"context": context, "integration_id": float64(githubActionsAppID)})
		}
		parameters["required_status_checks"] = rows
		return nil
	}
	return fmt.Errorf("ruleset has no required status checks")
}

func removeRequiredChecks(ruleset *GitHubRuleset) error {
	result := make([]map[string]any, 0, len(ruleset.Rules)-1)
	found := false
	for _, rule := range ruleset.Rules {
		if rule["type"] == "required_status_checks" {
			if found {
				return fmt.Errorf("ruleset has duplicate required status check rules")
			}
			found = true
			continue
		}
		result = append(result, rule)
	}
	if !found {
		return fmt.Errorf("ruleset has no required status checks")
	}
	ruleset.Rules = result
	return nil
}

func updatePullRequestRule(ruleset *GitHubRuleset) error {
	for _, rule := range ruleset.Rules {
		if rule["type"] != "pull_request" {
			continue
		}
		parameters, ok := rule["parameters"].(map[string]any)
		if !ok {
			return fmt.Errorf("pull request rule is malformed")
		}
		parameters["required_review_thread_resolution"] = true
		parameters["required_approving_review_count"] = float64(0)
		parameters["allowed_merge_methods"] = []any{"merge", "squash"}
		return nil
	}
	return fmt.Errorf("ruleset has no pull request rule")
}

func cloneRuleset(value GitHubRuleset) GitHubRuleset {
	body, _ := json.Marshal(value)
	var result GitHubRuleset
	_ = json.Unmarshal(body, &result)
	return result
}

func sameOrderedStrings(left, right []string) bool {
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

func canonicalJSON(value any) (json.RawMessage, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var normalized any
	if err := json.Unmarshal(body, &normalized); err != nil {
		return nil, err
	}
	return json.Marshal(normalized)
}

func canonicalHash(value any) (string, error) {
	body, err := canonicalJSON(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func policyBundleDigest(bundle GitHubPolicyBundle) (string, error) {
	bundle.Digest = ""
	return canonicalHash(bundle)
}
