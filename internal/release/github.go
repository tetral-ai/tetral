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
	ProtectedBranches    bool     `json:"protected_branches"`
	CustomBranchPolicies bool     `json:"custom_branch_policies"`
	Branches             []string `json:"branches"`
}

type EnvironmentPolicy struct {
	CanAdminsBypass   bool                   `json:"can_admins_bypass"`
	PreventSelfReview bool                   `json:"prevent_self_review"`
	Reviewers         []EnvironmentReviewer  `json:"reviewers"`
	BranchPolicy      DeploymentBranchPolicy `json:"deployment_branch_policy"`
}

type EnvironmentSnapshot struct {
	Policy        EnvironmentPolicy `json:"policy"`
	SecretNames   []string          `json:"secret_names"`
	VariableNames []string          `json:"variable_names"`
}

type EnvironmentMutation struct {
	Method           string            `json:"method"`
	Endpoint         string            `json:"endpoint"`
	ExpectedPreHash  string            `json:"expected_pre_state_sha256"`
	Target           EnvironmentPolicy `json:"target"`
	ExpectedPostHash string            `json:"expected_post_state_sha256"`
	Restore          EnvironmentPolicy `json:"restore"`
	ExpectedBackHash string            `json:"expected_restore_sha256"`
}

type EnvironmentBundle struct {
	Schema   string              `json:"schema"`
	PreState EnvironmentSnapshot `json:"pre_state"`
	Mutation EnvironmentMutation `json:"mutation"`
}

func BuildEnvironmentBundle(repository string, reviewer EnvironmentReviewer, pre EnvironmentSnapshot) (EnvironmentBundle, error) {
	if repository == "" || reviewer.ID < 1 || (reviewer.Type != "User" && reviewer.Type != "Team") {
		return EnvironmentBundle{}, fmt.Errorf("release environment identity is incomplete")
	}
	pre = normalizeEnvironmentSnapshot(pre)
	target := EnvironmentPolicy{
		CanAdminsBypass: false, PreventSelfReview: false,
		Reviewers:    []EnvironmentReviewer{reviewer},
		BranchPolicy: DeploymentBranchPolicy{CustomBranchPolicies: true, Branches: []string{"main"}},
	}
	preHash, err := ContentDigest(pre.Policy)
	if err != nil {
		return EnvironmentBundle{}, err
	}
	targetHash, err := ContentDigest(target)
	if err != nil {
		return EnvironmentBundle{}, err
	}
	return EnvironmentBundle{
		Schema: "tetral.release-environment-transition/v1", PreState: pre,
		Mutation: EnvironmentMutation{
			Method: "PUT", Endpoint: "repos/" + repository + "/environments/release",
			ExpectedPreHash: preHash, Target: target, ExpectedPostHash: targetHash,
			Restore: pre.Policy, ExpectedBackHash: preHash,
		},
	}, nil
}

func VerifyEnvironmentBundle(bundle EnvironmentBundle) error {
	if bundle.Schema != "tetral.release-environment-transition/v1" || bundle.Mutation.Method != "PUT" || bundle.Mutation.Endpoint == "" {
		return fmt.Errorf("release environment bundle identity is incomplete")
	}
	if hash, err := ContentDigest(bundle.PreState.Policy); err != nil || hash != bundle.Mutation.ExpectedPreHash || hash != bundle.Mutation.ExpectedBackHash {
		return fmt.Errorf("release environment pre-state hash differs")
	}
	if hash, err := ContentDigest(bundle.Mutation.Target); err != nil || hash != bundle.Mutation.ExpectedPostHash {
		return fmt.Errorf("release environment target hash differs")
	}
	target := bundle.Mutation.Target
	if target.CanAdminsBypass || target.PreventSelfReview || len(target.Reviewers) != 1 || target.Reviewers[0].ID < 1 || target.BranchPolicy.ProtectedBranches || !target.BranchPolicy.CustomBranchPolicies || len(target.BranchPolicy.Branches) != 1 || target.BranchPolicy.Branches[0] != "main" {
		return fmt.Errorf("release environment target is not the approved sole-main policy")
	}
	return nil
}

type PackageIdentity struct {
	Found                   bool     `json:"found"`
	Name                    string   `json:"name"`
	Organization            string   `json:"organization"`
	Visibility              string   `json:"visibility"`
	LinkedRepositoryID      int64    `json:"linked_repository_id"`
	ActionsRepositoryIDs    []int64  `json:"actions_repository_ids"`
	ExistingReferences      []string `json:"existing_references"`
	RepositoryTokenCanRead  bool     `json:"repository_token_can_read"`
	RepositoryTokenCanWrite bool     `json:"repository_token_can_write"`
	CreationAuthorized      bool     `json:"creation_authorized"`
}

func ValidatePackagePreflight(identity PackageIdentity, organization string, repositoryID int64, requireWrite bool) error {
	if !identity.Found {
		if identity.Name == "" || identity.Organization != organization || !identity.CreationAuthorized || !identity.RepositoryTokenCanRead || requireWrite && !identity.RepositoryTokenCanWrite {
			return fmt.Errorf("not-found release package lacks an exact creation target or repository authority")
		}
		if identity.Visibility != "" || identity.LinkedRepositoryID != 0 || len(identity.ActionsRepositoryIDs) != 0 || len(identity.ExistingReferences) != 0 {
			return fmt.Errorf("not-found release package contains fabricated live identity")
		}
		return nil
	}
	if identity.Name == "" || identity.Organization != organization || identity.Visibility != "public" || identity.LinkedRepositoryID != repositoryID || !identity.RepositoryTokenCanRead {
		return fmt.Errorf("release package ownership, visibility, linkage, or read authority differs")
	}
	allowed := false
	for _, id := range identity.ActionsRepositoryIDs {
		if id == repositoryID {
			allowed = true
		}
	}
	if !allowed || requireWrite && !identity.RepositoryTokenCanWrite {
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
	Schema       string            `json:"schema"`
	Organization string            `json:"organization"`
	RepositoryID int64             `json:"repository_id"`
	RequireWrite bool              `json:"require_write"`
	Packages     []PackageIdentity `json:"packages"`
}

func ValidatePackagePreflights(preflight PackagePreflight) error {
	if preflight.Schema != "tetral.release-package-preflight/v1" || preflight.Organization == "" || preflight.RepositoryID < 1 || len(preflight.Packages) == 0 {
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
	sort.Strings(snapshot.SecretNames)
	sort.Strings(snapshot.VariableNames)
	sort.Strings(snapshot.Policy.BranchPolicy.Branches)
	sort.Slice(snapshot.Policy.Reviewers, func(i, j int) bool {
		if snapshot.Policy.Reviewers[i].Type != snapshot.Policy.Reviewers[j].Type {
			return snapshot.Policy.Reviewers[i].Type < snapshot.Policy.Reviewers[j].Type
		}
		return snapshot.Policy.Reviewers[i].ID < snapshot.Policy.Reviewers[j].ID
	})
	return snapshot
}
