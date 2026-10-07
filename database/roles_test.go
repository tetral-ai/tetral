package database

import "testing"

func TestRoleContractDeclaresEveryServingDatabaseWorkload(t *testing.T) {
	contract, err := LoadRoleContract()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"api", "auth", "bridge", "cleanup", "event_stream", "git_proxy", "job_runner", "mcp_connector", "provider_gateway", "queue", "sandbox"}
	got := contract.WorkloadNames()
	if len(got) != len(want) {
		t.Fatalf("workload roles = %v; want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("workload roles = %v; want %v", got, want)
		}
	}
}
