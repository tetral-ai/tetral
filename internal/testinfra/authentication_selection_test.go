package testinfra

import (
	"slices"
	"testing"
)

func TestAuthenticationProofsUseNativeProfilesAndAffectedConsumers(t *testing.T) {
	root := repositoryRootForTest(t)
	inventory, err := LoadInventory()
	if err != nil {
		t.Fatal(err)
	}
	// These protocol/role/lifecycle roots must stay executable in ordinary native
	// verification. Dependency declarations cannot silently turn into Fast skips
	// or disappear from the affected public-consumer closure.
	required := map[string][]string{
		"TestOIDCKeycloakSDK":                                              {"keycloak", "postgresql", "docker", "sdk"},
		"TestOIDCKeycloakAuthRotation":                                     {"keycloak", "postgresql", "docker"},
		"TestOIDCReplicaAuthority":                                         {"postgresql", "docker"},
		"TestPostgreSQLOIDCExchange":                                       {"postgresql"},
		"TestPostgreSQLAuthPolicyCommand":                                  {"postgresql", "docker"},
		"TestAuthorityResolverDerivedKeysPreserveCeilingAndDurableLineage": {"postgresql"},
		"TestAuthPolicyImportAtomicNoopAndTerminalGrants":                  {"postgresql"},
		"TestAuthTokenPrunerRetentionAndBatch":                             {"postgresql"},
	}
	for name, dependencies := range required {
		found := false
		for _, declared := range inventory.GoTests {
			if declared.Name != name {
				continue
			}
			found = true
			for _, dependency := range dependencies {
				if !slices.Contains(declared.Dependencies, dependency) {
					t.Errorf("%s lost required native dependency %s", name, dependency)
				}
			}
		}
		if !found {
			t.Errorf("native inventory lost Auth proof %s", name)
		}
	}
	fast, excluded, err := fastGoSelections(root)
	if err != nil {
		t.Fatal(err)
	}
	for name := range required {
		for _, selection := range fast {
			if slices.Contains(selection.Tests, name) {
				t.Errorf("Fast unexpectedly executes native Auth proof %s", name)
			}
		}
		found := false
		for _, exclusion := range excluded {
			if exclusion.Runnable == name {
				found = true
			}
		}
		if !found {
			t.Errorf("Fast failed to account for excluded Auth proof %s", name)
		}
	}
	full, _, err := fullGoSelections(root, "authentication profile contract")
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for index := 0; index < 4; index++ {
		shard, err := SelectPlan(Plan{Profile: ProfileFull, Selections: full}, []string{"go"}, index, 4)
		if err != nil {
			t.Fatal(err)
		}
		for _, selection := range shard.Selections {
			for name, dependencies := range required {
				if !slices.Contains(selection.Tests, name) {
					continue
				}
				counts[name]++
				for _, dependency := range dependencies {
					if !slices.Contains(selection.Dependencies, dependency) || !slices.Contains(shard.Dependencies, dependency) {
						t.Errorf("CI shard omitted %s for %s", dependency, name)
					}
				}
			}
		}
	}
	for name := range required {
		if counts[name] != 1 {
			t.Errorf("Full Race shards execute %s %d times", name, counts[name])
		}
	}
	revision := Revision{ChangedPaths: []string{"internal/auth/authority_resolver.go"}}
	selected, err := affectedSelections(root, inventory, &revision)
	if err != nil {
		t.Fatal(err)
	}
	if revision.FullFallbackCause != "" {
		t.Fatalf("known Auth owner fell back to Full: %s", revision.FullFallbackCause)
	}
	var owners []string
	for _, selection := range selected {
		if selection.Group == "go" {
			owners = selection.Packages
		}
	}
	for _, owner := range []string{"internal/auth", "internal/httpapi", "internal/memory", "internal/eventstream", "services/auth", "services/event-stream", "integration"} {
		if !slices.Contains(owners, "github.com/tetral-ai/tetral/"+owner) {
			t.Errorf("Auth change omitted public consumer %s", owner)
		}
	}
	expanded, err := expandAffectedGoSelection(root, selected)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"TestOIDCKeycloakSDK", "TestOIDCKeycloakAuthRotation", "TestOIDCReplicaAuthority"} {
		found := false
		for _, selection := range expanded {
			if !slices.Contains(selection.Tests, name) {
				continue
			}
			found = true
			for _, dependency := range required[name] {
				if !slices.Contains(selection.Dependencies, dependency) {
					t.Errorf("Affected lost %s dependency %s", name, dependency)
				}
			}
		}
		if !found {
			t.Errorf("Affected lost actual Auth consumer %s", name)
		}
	}
}
