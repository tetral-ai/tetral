package tetralcleanup

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/workspace"
)

type failedWorkspaceList struct{}

func (failedWorkspaceList) ListIDs(context.Context) ([]workspace.ID, error) {
	return nil, context.DeadlineExceeded
}

func TestCleanupOperationDurationsIncludeFailedJobBoundary(t *testing.T) {
	metrics := NewSchedulerMetrics()
	err := ClaimDueAcrossWorkspaces(context.Background(), failedWorkspaceList{}, &recordingCleanupClaimer{}, 17, nil, metrics)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("changed job outcome: %v", err)
	}
	if !strings.Contains(metrics.Operations.Text(), `tetral_operation_duration_seconds_count{operation="claim_due_across_workspaces",outcome="timeout",service="cleanup"} 1`) {
		t.Fatalf("missing failed job duration: %s", metrics.Operations.Text())
	}
	if err := ClaimDueAcrossWorkspaces(context.Background(), cleanupStaticWorkspaceLister{"workspace-not-a-label"}, &recordingCleanupClaimer{}, 17, nil, metrics); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(metrics.Operations.Text(), "workspace-not-a-label") {
		t.Fatal("workspace became a label")
	}
	if !strings.Contains(metrics.Operations.Text(), `operation="claim_due_across_workspaces",outcome="success",service="cleanup"} 1`) {
		t.Fatal("success missing")
	}
}
