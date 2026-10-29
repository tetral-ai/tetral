package computerhub

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workload"
)

func TestFirstAnsweredReadyRecordsTheReportedCLIColdStart(t *testing.T) {
	lines := &lockedBuffer{}
	telemetry := workload.NewTelemetry(workload.TelemetryOptions{Service: ServiceName, Logger: workload.NewLogger(lines, ServiceName, "test", "unit")})
	t.Cleanup(workload.InstallTelemetry(telemetry))
	startup := protocol.Some(protocol.ReadyStartup{ConfigMS: 35, ConnectMS: 410, ReadyMS: 12})

	f := newHubFixture(t, 720*time.Hour)
	cli := f.userInstance(time.Time{})
	stream := f.open(cli)
	ready := readyOf(stream.connectionID, "ready_1", "1.0.0")
	ready.Startup = startup
	for range 2 { // the second send is a replay
		if response, err := cli.Ready(f.ctx, ready); err != nil || response.Status != http.StatusOK {
			t.Fatalf("ready = %d %s, %v", response.Status, response.Body, err)
		}
	}
	next := f.secondCredential(cli, time.Time{})
	if status, _ := f.do(http.MethodDelete, "/v1/instances/"+cli.InstanceID+"/credentials/"+cli.CredentialID, nil, ""); status != http.StatusNoContent {
		t.Fatalf("revoke = %d", status)
	}
	f.acceptReady(next, f.open(next), "ready_1")

	completed := coldStartRecords(t, lines.String())
	if len(completed) != 1 {
		t.Fatalf("cold_start.completed records = %d; want 1", len(completed))
	}
	record := completed[0]
	if record["cold_start.kind"] != "cli" || record["cold_start.start_kind"] != "new" || record["outcome"] != "ok" ||
		record["config_ms"] != float64(35) || record["connect_ms"] != float64(410) || record["ready_ms"] != float64(12) || record["register_ms"] == nil {
		t.Fatalf("cold start record = %v", record)
	}
	samples := coldStartSamples(t, telemetry)
	for _, phase := range []string{"config", "connect", "ready", "register"} {
		if samples["phase "+phase] != 1 {
			t.Fatalf("phase %s samples = %v; want 1", phase, samples["phase "+phase])
		}
	}
	if samples["scope total"] != 1 || samples["scope engine"] != 1 || samples["sum total"] != samples["sum engine"] {
		t.Fatalf("cold start samples = %v; want one total and one equal engine sample", samples)
	}

	refusing := newHubFixture(t, 720*time.Hour, withCLIVersions("1.0.0", "1.0.0"))
	old := refusing.userInstance(time.Time{})
	oldStream := refusing.open(old)
	refused := readyOf(oldStream.connectionID, "ready_1", "0.9.0")
	refused.Startup = startup
	response, err := old.Ready(refusing.ctx, refused)
	requireClientError(t, response, err, protocol.ErrorCLIUpgradeRequired)
	completed = coldStartRecords(t, lines.String())
	if len(completed) != 2 || completed[1]["outcome"] != "cli_upgrade_required" {
		t.Fatalf("cold start records = %v; want a second one ended by cli_upgrade_required", completed)
	}
	if after := coldStartSamples(t, telemetry); after["scope total"] != 1 || after["phase register"] != 1 {
		t.Fatalf("the refused cold start observed samples: %v", after)
	}
}

func coldStartRecords(t *testing.T, logs string) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs), "\n") {
		var record map[string]any
		if line == "" || json.Unmarshal([]byte(line), &record) != nil {
			continue
		}
		if record["event"] == "cold_start.completed" && record["cold_start.kind"] == "cli" {
			records = append(records, record)
		}
	}
	return records
}

// coldStartSamples counts the cli cold start samples by phase and scope and
// sums the two scopes.
func coldStartSamples(t *testing.T, telemetry *workload.Telemetry) map[string]float64 {
	t.Helper()
	metrics, err := telemetry.Collector()(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	samples := map[string]float64{}
	for _, metric := range metrics {
		labels := map[string]string{}
		for _, label := range metric.Labels {
			labels[label.Name] = label.Value
		}
		if labels["kind"] != "cli" || labels["service"] != ServiceName {
			continue
		}
		switch metric.Name {
		case "cold_start_phase_seconds_count":
			samples["phase "+labels["phase"]] = metric.Value
		case "cold_start_seconds_count":
			samples["scope "+labels["scope"]] = metric.Value
		case "cold_start_seconds_sum":
			samples["sum "+labels["scope"]] = metric.Value
		}
	}
	return samples
}
