package testinfra

import (
	"strings"
	"testing"
)

func TestVerifyUpstreamNeedsRejectsFailureCancellationAndMissing(t *testing.T) {
	expected := []string{"one", "two"}
	if err := VerifyUpstreamNeeds(`{"one":{"result":"success"},"two":{"result":"success"}}`, expected); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{
		`{"one":{"result":"success"}}`,
		`{"one":{"result":"success"},"two":{"result":"failure"}}`,
		`{"one":{"result":"success"},"two":{"result":"cancelled"}}`,
		`{"one":{"result":"success"},"two":{"result":"skipped"}}`,
	} {
		if err := VerifyUpstreamNeeds(body, expected); err == nil {
			t.Fatalf("invalid dependency results passed: %s", body)
		}
	}
}

func TestVerifyLiveGateFactsRejectsSupersededAndWrongCheck(t *testing.T) {
	want := gateFixtureExpectation()
	facts := GateLiveFacts{
		CurrentHeadSHA: want.EventHeadSHA, CurrentBaseSHA: want.EventBaseSHA,
		RunID: 123, RunAttempt: 1, RunName: want.Workflow, RunPath: ".github/workflows/pull-request-verification.yml",
		RunHeadSHA: want.EventHeadSHA, CheckSuiteID: 7, CheckHeadSHA: want.RequiredCheckSHA, SourceAppID: githubActionsAppID,
		GateChecks: []ShadowCheck{{ID: 8, Name: "Merge Gate", HeadSHA: want.RequiredCheckSHA, AppID: githubActionsAppID, Status: "in_progress"}},
	}
	if err := VerifyLiveGateFacts(want, facts); err != nil {
		t.Fatal(err)
	}
	facts.CurrentHeadSHA = strings.Repeat("9", 40)
	if err := VerifyLiveGateFacts(want, facts); err == nil {
		t.Fatal("superseded PR head passed")
	}
	facts.CurrentHeadSHA = want.EventHeadSHA
	facts.GateChecks[0].AppID++
	if err := VerifyLiveGateFacts(want, facts); err == nil {
		t.Fatal("wrong check App passed")
	}
}
