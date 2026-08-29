package release

import (
	"fmt"
	"sort"
)

type EnvironmentReviewer struct {
	Type string `json:"type"`
	ID   int64  `json:"id"`
}

type DeploymentBranchPolicy struct {
	ProtectedBranches    bool `json:"protected_branches"`
	CustomBranchPolicies bool `json:"custom_branch_policies"`
}

type DeploymentBranchEntry struct {
	ID   int64  `json:"id,omitempty"`
	Name string `json:"name"`
	Type string `json:"type"`
}

type EnvironmentPolicy struct {
	CanAdminsBypass   bool                    `json:"can_admins_bypass"`
	PreventSelfReview bool                    `json:"prevent_self_review"`
	Reviewers         []EnvironmentReviewer   `json:"reviewers"`
	BranchPolicy      *DeploymentBranchPolicy `json:"deployment_branch_policy"`
}

type EnvironmentSnapshot struct {
	Policy        EnvironmentPolicy       `json:"policy"`
	Branches      []DeploymentBranchEntry `json:"branches"`
	SecretNames   []string                `json:"secret_names"`
	VariableNames []string                `json:"variable_names"`
}

type EnvironmentOperation struct {
	Name         string `json:"name"`
	Method       string `json:"method"`
	Endpoint     string `json:"endpoint"`
	Body         any    `json:"body,omitempty"`
	ReadbackHash string `json:"readback_sha256,omitempty"`
}

type EnvironmentAPIUpdate struct {
	PreventSelfReview bool                    `json:"prevent_self_review"`
	Reviewers         []EnvironmentReviewer   `json:"reviewers"`
	BranchPolicy      *DeploymentBranchPolicy `json:"deployment_branch_policy"`
}

type EnvironmentBundle struct {
	Schema            string                 `json:"schema"`
	PreState          EnvironmentSnapshot    `json:"pre_state"`
	TargetState       EnvironmentSnapshot    `json:"target_state"`
	ApplyOperations   []EnvironmentOperation `json:"apply_operations"`
	RestoreOperations []EnvironmentOperation `json:"restore_operations"`
}

func BuildEnvironmentBundle(repository string, reviewer EnvironmentReviewer, pre EnvironmentSnapshot) (EnvironmentBundle, error) {
	if repository == "" || reviewer.ID < 1 || (reviewer.Type != "User" && reviewer.Type != "Team") {
		return EnvironmentBundle{}, fmt.Errorf("release environment identity is incomplete")
	}
	pre = normalizeEnvironmentSnapshot(pre)
	target := EnvironmentPolicy{
		CanAdminsBypass: false, PreventSelfReview: false,
		Reviewers:    []EnvironmentReviewer{reviewer},
		BranchPolicy: &DeploymentBranchPolicy{CustomBranchPolicies: true},
	}
	targetSnapshot := normalizeEnvironmentSnapshot(EnvironmentSnapshot{Policy: target, Branches: []DeploymentBranchEntry{{Name: "main", Type: "branch"}}, SecretNames: append([]string(nil), pre.SecretNames...), VariableNames: append([]string(nil), pre.VariableNames...)})
	targetHash, err := environmentPolicyDigest(targetSnapshot)
	if err != nil {
		return EnvironmentBundle{}, err
	}
	preHash, err := environmentPolicyDigest(pre)
	if err != nil {
		return EnvironmentBundle{}, err
	}
	base := "repos/" + repository + "/environments/release"
	settingsURL := "https://github.com/" + repository + "/settings/environments"
	bypassState := pre
	bypassState.Policy.CanAdminsBypass = false
	bypassStateHash, err := environmentPolicyDigest(bypassState)
	if err != nil {
		return EnvironmentBundle{}, err
	}
	intermediate := normalizeEnvironmentSnapshot(EnvironmentSnapshot{Policy: target, Branches: append([]DeploymentBranchEntry(nil), pre.Branches...), SecretNames: append([]string(nil), pre.SecretNames...), VariableNames: append([]string(nil), pre.VariableNames...)})
	intermediateHash, err := environmentPolicyDigest(intermediate)
	if err != nil {
		return EnvironmentBundle{}, err
	}
	apply := []EnvironmentOperation{
		{Name: "disallow-administrator-bypass", Method: "UI", Endpoint: settingsURL, Body: map[string]bool{"can_admins_bypass": false}, ReadbackHash: bypassStateHash},
		{Name: "set-environment-policy", Method: "PUT", Endpoint: base, Body: environmentAPIUpdate(target), ReadbackHash: intermediateHash},
	}
	remaining := append([]DeploymentBranchEntry(nil), pre.Branches...)
	for _, branch := range pre.Branches {
		remaining = removeBranchEntry(remaining, branch.ID)
		readback := normalizeEnvironmentSnapshot(EnvironmentSnapshot{Policy: target, Branches: append([]DeploymentBranchEntry(nil), remaining...), SecretNames: append([]string(nil), pre.SecretNames...), VariableNames: append([]string(nil), pre.VariableNames...)})
		readbackHash, hashErr := environmentPolicyDigest(readback)
		if hashErr != nil {
			return EnvironmentBundle{}, hashErr
		}
		apply = append(apply, EnvironmentOperation{Name: "remove-pre-state-branch-policy", Method: "DELETE", Endpoint: fmt.Sprintf("%s/deployment-branch-policies/%d", base, branch.ID), ReadbackHash: readbackHash})
	}
	apply = append(apply, EnvironmentOperation{Name: "create-main-branch-policy", Method: "POST", Endpoint: base + "/deployment-branch-policies", Body: DeploymentBranchEntry{Name: "main", Type: "branch"}, ReadbackHash: targetHash})
	targetWithoutMain := normalizeEnvironmentSnapshot(EnvironmentSnapshot{Policy: target, SecretNames: append([]string(nil), pre.SecretNames...), VariableNames: append([]string(nil), pre.VariableNames...)})
	targetWithoutMainHash, err := environmentPolicyDigest(targetWithoutMain)
	if err != nil {
		return EnvironmentBundle{}, err
	}
	restoreBase := normalizeEnvironmentSnapshot(EnvironmentSnapshot{Policy: pre.Policy, SecretNames: append([]string(nil), pre.SecretNames...), VariableNames: append([]string(nil), pre.VariableNames...)})
	restoreBaseHash, err := environmentPolicyDigest(restoreBase)
	if err != nil {
		return EnvironmentBundle{}, err
	}
	restore := []EnvironmentOperation{{Name: "remove-main-branch-policy", Method: "DELETE", Endpoint: base + "/deployment-branch-policies/{target_policy_id}", ReadbackHash: targetWithoutMainHash}, {Name: "restore-environment-policy", Method: "PUT", Endpoint: base, Body: environmentAPIUpdate(pre.Policy), ReadbackHash: restoreBaseHash}}
	restoredBranches := []DeploymentBranchEntry{}
	for _, branch := range pre.Branches {
		restoredBranches = append(restoredBranches, DeploymentBranchEntry{Name: branch.Name, Type: branch.Type})
		readback := normalizeEnvironmentSnapshot(EnvironmentSnapshot{Policy: pre.Policy, Branches: append([]DeploymentBranchEntry(nil), restoredBranches...), SecretNames: append([]string(nil), pre.SecretNames...), VariableNames: append([]string(nil), pre.VariableNames...)})
		readbackHash, hashErr := environmentPolicyDigest(readback)
		if hashErr != nil {
			return EnvironmentBundle{}, hashErr
		}
		restore = append(restore, EnvironmentOperation{Name: "restore-branch-policy", Method: "POST", Endpoint: base + "/deployment-branch-policies", Body: DeploymentBranchEntry{Name: branch.Name, Type: branch.Type}, ReadbackHash: readbackHash})
	}
	restore = append(restore, EnvironmentOperation{Name: "restore-administrator-bypass", Method: "UI", Endpoint: settingsURL, Body: map[string]bool{"can_admins_bypass": pre.Policy.CanAdminsBypass}, ReadbackHash: preHash})
	return EnvironmentBundle{
		Schema: "tetral.release-environment-transition/v1", PreState: pre, TargetState: targetSnapshot,
		ApplyOperations: apply, RestoreOperations: restore,
	}, nil
}

func environmentAPIUpdate(policy EnvironmentPolicy) EnvironmentAPIUpdate {
	return EnvironmentAPIUpdate{PreventSelfReview: policy.PreventSelfReview, Reviewers: policy.Reviewers, BranchPolicy: policy.BranchPolicy}
}

func removeBranchEntry(entries []DeploymentBranchEntry, id int64) []DeploymentBranchEntry {
	result := make([]DeploymentBranchEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.ID != id {
			result = append(result, entry)
		}
	}
	return result
}

func environmentPolicyDigest(snapshot EnvironmentSnapshot) (string, error) {
	snapshot = normalizeEnvironmentSnapshot(snapshot)
	for index := range snapshot.Branches {
		snapshot.Branches[index].ID = 0
	}
	return ContentDigest(snapshot)
}

func VerifyEnvironmentBundle(bundle EnvironmentBundle) error {
	if bundle.Schema != "tetral.release-environment-transition/v1" || len(bundle.ApplyOperations) < 2 || len(bundle.RestoreOperations) < 2 {
		return fmt.Errorf("release environment bundle identity is incomplete")
	}
	target := bundle.TargetState
	if target.Policy.CanAdminsBypass || target.Policy.PreventSelfReview || len(target.Policy.Reviewers) != 1 || target.Policy.Reviewers[0].ID < 1 || target.Policy.BranchPolicy == nil || target.Policy.BranchPolicy.ProtectedBranches || !target.Policy.BranchPolicy.CustomBranchPolicies || len(target.Branches) != 1 || target.Branches[0].Name != "main" || target.Branches[0].Type != "branch" {
		return fmt.Errorf("release environment target is not the approved sole-main policy")
	}
	if bundle.ApplyOperations[0].Method != "UI" || bundle.ApplyOperations[1].Method != "PUT" || bundle.ApplyOperations[len(bundle.ApplyOperations)-1].Method != "POST" || bundle.RestoreOperations[0].Method != "DELETE" || bundle.RestoreOperations[1].Method != "PUT" || bundle.RestoreOperations[len(bundle.RestoreOperations)-1].Method != "UI" {
		return fmt.Errorf("release environment operation order is unsafe")
	}
	targetHash, err := environmentPolicyDigest(target)
	if err != nil || bundle.ApplyOperations[len(bundle.ApplyOperations)-1].ReadbackHash != targetHash {
		return fmt.Errorf("release environment target readback is incomplete")
	}
	preHash, err := environmentPolicyDigest(bundle.PreState)
	if err != nil || bundle.RestoreOperations[len(bundle.RestoreOperations)-1].ReadbackHash != preHash {
		return fmt.Errorf("release environment restore readback is incomplete")
	}
	return nil
}

type PackageIdentity struct {
	Found                bool     `json:"found"`
	Name                 string   `json:"name"`
	Organization         string   `json:"organization"`
	Visibility           string   `json:"visibility"`
	LinkedRepositoryID   int64    `json:"linked_repository_id"`
	ActionsRepositoryIDs []int64  `json:"actions_repository_ids"`
	ExistingReferences   []string `json:"existing_references"`
	CreationAuthorized   bool     `json:"creation_authorized"`
}

func ValidatePackagePreflight(identity PackageIdentity, organization string, repositoryID int64, requireWrite bool) error {
	if !identity.Found {
		if identity.Name == "" || identity.Organization != organization || !identity.CreationAuthorized {
			return fmt.Errorf("not-found release package lacks an exact creation target or repository authority")
		}
		if identity.Visibility != "" || identity.LinkedRepositoryID != 0 || len(identity.ActionsRepositoryIDs) != 0 || len(identity.ExistingReferences) != 0 {
			return fmt.Errorf("not-found release package contains fabricated live identity")
		}
		return nil
	}
	if identity.Name == "" || identity.Organization != organization || identity.Visibility != "public" || identity.LinkedRepositoryID != repositoryID {
		return fmt.Errorf("release package ownership, visibility, linkage, or read authority differs")
	}
	allowed := false
	for _, id := range identity.ActionsRepositoryIDs {
		if id == repositoryID {
			allowed = true
		}
	}
	if !allowed {
		return fmt.Errorf("release package Actions authority is insufficient")
	}
	for _, reference := range identity.ExistingReferences {
		if reference == "latest" {
			return fmt.Errorf("release package contains a moving latest reference")
		}
	}
	return nil
}

type PackagePreflight struct {
	Schema                string            `json:"schema"`
	Organization          string            `json:"organization"`
	RepositoryID          int64             `json:"repository_id"`
	RequireWrite          bool              `json:"require_write"`
	DeclaredJobPermission string            `json:"declared_job_permission"`
	ReadbackComplete      bool              `json:"readback_complete"`
	Packages              []PackageIdentity `json:"packages"`
}

func ValidatePackagePreflights(preflight PackagePreflight) error {
	if preflight.Schema != "tetral.release-package-preflight/v1" || preflight.Organization == "" || preflight.RepositoryID < 1 || len(preflight.Packages) == 0 || !preflight.ReadbackComplete || preflight.RequireWrite && preflight.DeclaredJobPermission != "write" || !preflight.RequireWrite && preflight.DeclaredJobPermission != "read" {
		return fmt.Errorf("release package preflight identity is incomplete")
	}
	seen := map[string]bool{}
	for _, identity := range preflight.Packages {
		if seen[identity.Name] {
			return fmt.Errorf("release package %q appears more than once", identity.Name)
		}
		seen[identity.Name] = true
		if err := ValidatePackagePreflight(identity, preflight.Organization, preflight.RepositoryID, preflight.RequireWrite); err != nil {
			return fmt.Errorf("release package %q: %w", identity.Name, err)
		}
	}
	return nil
}

func normalizeEnvironmentSnapshot(snapshot EnvironmentSnapshot) EnvironmentSnapshot {
	if snapshot.Policy.Reviewers == nil {
		snapshot.Policy.Reviewers = []EnvironmentReviewer{}
	}
	if snapshot.Branches == nil {
		snapshot.Branches = []DeploymentBranchEntry{}
	}
	if snapshot.SecretNames == nil {
		snapshot.SecretNames = []string{}
	}
	if snapshot.VariableNames == nil {
		snapshot.VariableNames = []string{}
	}
	sort.Strings(snapshot.SecretNames)
	sort.Strings(snapshot.VariableNames)
	sort.Slice(snapshot.Branches, func(i, j int) bool {
		if snapshot.Branches[i].Name != snapshot.Branches[j].Name {
			return snapshot.Branches[i].Name < snapshot.Branches[j].Name
		}
		return snapshot.Branches[i].Type < snapshot.Branches[j].Type
	})
	sort.Slice(snapshot.Policy.Reviewers, func(i, j int) bool {
		if snapshot.Policy.Reviewers[i].Type != snapshot.Policy.Reviewers[j].Type {
			return snapshot.Policy.Reviewers[i].Type < snapshot.Policy.Reviewers[j].Type
		}
		return snapshot.Policy.Reviewers[i].ID < snapshot.Policy.Reviewers[j].ID
	})
	return snapshot
}
