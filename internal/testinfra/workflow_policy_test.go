package testinfra

import "testing"

func TestVerificationWorkflowStructure(t *testing.T) {
	root := testRepositoryRoot(t)
	if err := VerifyWorkflowSkeletons(root); err != nil {
		t.Fatal(err)
	}
	if err := VerifyPullRequestWorkflow(root); err != nil {
		t.Fatal(err)
	}
	if err := VerifyMainBranchWorkflow(root); err != nil {
		t.Fatal(err)
	}
	if err := VerifyScheduledWorkflow(root); err != nil {
		t.Fatal(err)
	}
}
