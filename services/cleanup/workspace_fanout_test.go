// These tests pin service-owned workspace fan-out and domain validation;
// command tests cover only startup wiring, logging, and metrics export.
package tetralcleanup

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestClaimDueAcrossWorkspacesVisitsEveryDiscoveredWorkspace(t *testing.T) {
	claimer := &recordingCleanupClaimer{}
	var observed []workspace.ID
	err := ClaimDueAcrossWorkspaces(
		context.Background(),
		cleanupStaticWorkspaceLister{"ws_alpha", "ws_beta"},
		claimer,
		17,
		func(workspaceID workspace.ID, _ int, _ time.Duration) { observed = append(observed, workspaceID) },
	)
	if err != nil {
		t.Fatalf("ClaimDueAcrossWorkspaces: %v", err)
	}
	if !reflect.DeepEqual(claimer.requests, []ClaimDueRequest{
		{WorkspaceID: "ws_alpha", Limit: 17},
		{WorkspaceID: "ws_beta", Limit: 17},
	}) {
		t.Fatalf("claim requests = %#v; want every workspace", claimer.requests)
	}
	if !reflect.DeepEqual(observed, []workspace.ID{"ws_alpha", "ws_beta"}) {
		t.Fatalf("observed = %v; want every workspace", observed)
	}
}

type cleanupStaticWorkspaceLister []workspace.ID

func (l cleanupStaticWorkspaceLister) ListIDs(context.Context) ([]workspace.ID, error) {
	return append([]workspace.ID(nil), l...), nil
}

type recordingCleanupClaimer struct {
	requests []ClaimDueRequest
}

func (c *recordingCleanupClaimer) ClaimDue(_ context.Context, request ClaimDueRequest) ([]ClaimedCleanupJob, error) {
	c.requests = append(c.requests, request)
	return nil, nil
}
