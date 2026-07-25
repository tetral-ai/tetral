package driver

import (
	"testing"

	apiclient "github.com/daytonaio/daytona/libs/api-client-go"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/daytona"

	"github.com/tetral-ai/tetral/internal/sandbox"
)

func TestDaytonaSandboxStatusMapsProviderStatesToTetralStates(t *testing.T) {
	tests := []struct {
		name string
		in   apiclient.SandboxState
		want sandbox.Status
	}{
		{name: "started", in: apiclient.SANDBOXSTATE_STARTED, want: sandbox.StatusActive},
		{name: "stopped", in: apiclient.SANDBOXSTATE_STOPPED, want: sandbox.StatusStopped},
		{name: "archived", in: apiclient.SANDBOXSTATE_ARCHIVED, want: sandbox.StatusArchived},
		{name: "starting", in: apiclient.SANDBOXSTATE_STARTING, want: sandbox.StatusResuming},
		{name: "destroyed", in: apiclient.SANDBOXSTATE_DESTROYED, want: sandbox.StatusReleased},
		{name: "error", in: apiclient.SANDBOXSTATE_ERROR, want: sandbox.StatusFailed},
		{name: "unknown", in: apiclient.SANDBOXSTATE_UNKNOWN, want: sandbox.StatusFailed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := daytonaSandboxStatus(&daytona.Sandbox{State: tc.in})
			if got != tc.want {
				t.Fatalf("daytonaSandboxStatus(%s) = %s; want %s", tc.in, got, tc.want)
			}
		})
	}
}
