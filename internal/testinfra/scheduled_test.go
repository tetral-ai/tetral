package testinfra

import (
	"errors"
	"testing"
)

func TestScheduledConcurrencyPlanSelectsOnlyNamedOwners(t *testing.T) {
	plan := Plan{Selections: []Selection{
		{Group: "go", Packages: []string{"github.com/tetral-ai/tetral/internal/queue"}, Dependencies: []string{"postgresql"}},
		{Group: "go", Packages: []string{"github.com/tetral-ai/tetral/internal/sessionevent"}, Dependencies: []string{"postgresql"}},
		{Group: "go", Packages: []string{"github.com/tetral-ai/tetral/services/bridge"}, Dependencies: []string{"postgresql", "minio"}},
		{Group: "go", Packages: []string{"github.com/tetral-ai/tetral/internal/files"}, Dependencies: []string{"postgresql"}},
		{Group: "runtime"},
	}}
	selected, err := scheduledConcurrencyPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected.Selections) != 3 {
		t.Fatalf("selected %d owners, want 3", len(selected.Selections))
	}
	for _, selection := range selected.Selections {
		if selection.Group != "go" || selection.Packages[0] == "github.com/tetral-ai/tetral/internal/files" {
			t.Fatalf("unexpected scheduled selection: %+v", selection)
		}
	}
}

func TestScheduledConcurrencyPlanFailsClosedWhenOwnerDisappears(t *testing.T) {
	if _, err := scheduledConcurrencyPlan(Plan{}); err == nil {
		t.Fatal("missing scheduled owners were accepted")
	}
}

func TestScheduledReportPreservesFirstFailureAfterLaterPass(t *testing.T) {
	report := ScheduledReport{Status: "pass"}
	recordScheduledAttempt(&report, Result{Status: "fail"}, errors.New("first red"), 1)
	recordScheduledAttempt(&report, Result{Status: "pass"}, nil, 2)
	if report.Status != "fail" || report.FirstFailureAttempt != 1 || !report.LaterAttemptPassed {
		t.Fatalf("first failure was erased: %+v", report)
	}
}
