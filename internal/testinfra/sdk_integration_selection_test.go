package testinfra

import (
	"slices"
	"testing"
)

func TestSDKIntegrationOwningCommandEncompassesFixedChildBudgets(t *testing.T) {
	for _, tc := range []struct {
		name, test, timeout string
	}{
		{"registered wrapper", "TestForkSDKIntegrationCompatibilityProofs", "-timeout=30m"},
		{"ordinary OIDC root", "TestOIDCKeycloakSDK", "-timeout=25m"},
		{"unregistered similar name", "TestForkSDKIntegrationCompatibilityProofsOther", "-timeout=25m"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selection := Selection{Group: "go", Packages: []string{"github.com/tetral-ai/tetral/integration"}, Tests: []string{tc.test}}
			for _, profile := range []Profile{ProfileFull, ProfileAffected} {
				execution := goTestExecutionArguments(profile, selection)
				commands, err := commandsForSelection(Plan{Profile: profile}, selection, "", "", "")
				if err != nil {
					t.Fatal(err)
				}
				for _, arguments := range [][]string{execution, commands[0].Arguments} {
					if !slices.Contains(arguments, tc.timeout) || !slices.Contains(arguments, "-race") {
						t.Fatalf("owning native command lost scoped budget: %v", arguments)
					}
				}
			}
			if slices.Contains(goTestExecutionArguments(ProfileFast, selection), tc.timeout) {
				t.Fatal("Fast must retain compile-only behavior")
			}
		})
	}
}
