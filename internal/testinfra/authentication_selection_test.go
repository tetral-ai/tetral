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
	// These protocol/role/lifecycle roots must stay declared with their native
	// dependencies, which keeps them out of Fast and in exactly one race shard
	// (TestDeclaredGoTestsLeaveFastAndRunOnceAcrossRaceShards), and must stay
	// in the affected public-consumer closure.
	required := map[string][]string{
		"TestForkSDKIntegrationCompatibilityProofs":                        {"keycloak", "postgresql", "minio", "nats", "docker", "bun-workspaces", "sdk"},
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
	for _, name := range []string{"TestForkSDKIntegrationCompatibilityProofs", "TestOIDCKeycloakSDK", "TestOIDCKeycloakAuthRotation", "TestOIDCReplicaAuthority"} {
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
