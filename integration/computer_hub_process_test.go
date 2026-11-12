// Layer 3: two computer-hub processes, one killed for real, Bridge and the
// public API over a real PostgreSQL, through the test CLI.
package integration

import (
	"encoding/hex"
	"errors"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/enginetest"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

// withHubProcesses starts each hub server as the built command instead of
// an in-process server. The command opens its database as production does,
// so the test runs under isolatedTLSPostgreSQLRoot.
func withHubProcesses(t *testing.T) machineToolsOption {
	t.Helper()
	binary := builtCommand(t, "./services/computer-hub/cmd/tetral-computer-hub")
	return func(f *machineTools) { f.hubBinary = binary }
}

// hubCommandEnvironment is the environment of the computer-hub command as
// pod on addresses, apart from its database. No test of these processes
// transfers content.
func hubCommandEnvironment(addresses []string, principalPublicKey, pod string) map[string]string {
	env := map[string]string{
		"TETRAL_COMPUTER_HUB_HTTP_ADDR":                 addresses[0],
		"TETRAL_COMPUTER_HUB_METRICS_ADDR":              addresses[1],
		"TETRAL_HTTP_TRANSPORT":                         "plaintext",
		"TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64": principalPublicKey,
		"TETRAL_COMPUTER_HUB_PAGE_TOKEN_KEY":            hex.EncodeToString(make([]byte, 32)),
		"TETRAL_COMPUTER_HUB_POD_NAME":                  pod,
		"TETRAL_COMPUTER_HUB_SERVER_URL":                "https://api.tetral.test",
		"TETRAL_COMPUTER_HUB_GIT_PROXY_URL":             "https://git.tetral.test",
		"TETRAL_COMPUTER_HUB_STREAM_DRAIN_WINDOW":       "1s",
	}
	maps.Copy(env, commandBlobEnvironment())
	return env
}

// startHubProcess starts the built command as pod and returns its URL once
// it is ready.
func (f *machineTools) startHubProcess(pod string) string {
	f.t.Helper()
	addresses, release := publicReserveAddresses(f.t.(*testing.T), 2)
	release()
	env := hubCommandEnvironment(addresses, f.signer.PublicKeyBase64(), pod)
	maps.Copy(env, map[string]string{
		"TETRAL_DATABASE_URL":             storagetest.RuntimeDatabaseURL(f.t, f.hubPool),
		"TETRAL_DATABASE_TLS_CA_PATH":     filepath.Join(os.Getenv(envIsolatedTLSPGCerts), "ca.pem"),
		"TETRAL_DATABASE_TLS_SERVER_NAME": "postgres.transport.test",
		"TETRAL_DEPLOYMENT_ENVIRONMENT":   "test",
		"TETRAL_SERVICE_VERSION":          "process-fixture",
	})
	process := startCommand(f.t, pod, f.hubBinary, env)
	f.hubProcesses = append(f.hubProcesses, process)
	url := "http://" + addresses[0]
	process.ready(f.t, url+"/ready")
	return url
}

// Two computer-hub processes serve one database. When the one holding a
// machine's stream is killed, the machine's connection lives on: the instance
// reads it online, a call admitted meanwhile is stored, and the resume on the
// other process keeps the connection's ID and lease, moves it to that pod and
// writes every unreported instruction once, in the order issued, the call
// running across the kill first. Heartbeats and reports are accepted there,
// both calls settle with their results, and a later commit's notice precedes
// the next call. A machine whose stream is on the surviving process keeps it.
// Each process the test did not kill exits 0 within 20 seconds of SIGTERM.
func TestComputerHubProcessesKeepAConnectionThroughAKilledPod(t *testing.T) {
	isolatedTLSPostgreSQLRoot(t, "computer_hub_process_assertion=resumed_after_kill", isolatedTLSRootBudget, func(t *testing.T) {
		f := newMachineTools(t, withHubProcesses(t))
		on := func(hub int, cli *enginetest.CLI) *enginetest.CLI {
			at := *cli
			at.BaseURL = f.hubs[hub]
			return &at
		}
		online := func(when string, cli *enginetest.CLI, connectionID string) {
			t.Helper()
			status, body := f.controllerOn(1, http.MethodGet, "/v1/instances/"+cli.InstanceID, "")
			if status != http.StatusOK || !strings.Contains(string(body), `"id":"`+connectionID+`"`) || !strings.Contains(string(body), `"state":"online"`) {
				t.Fatalf("instance %s = %d %s; want connection %s online", when, status, body, connectionID)
			}
		}
		beat := func(cli *enginetest.CLI, connectionID string, seq int64) {
			t.Helper()
			response, err := cli.Heartbeat(f.ctx, protocol.Heartbeat{ConnectionID: connectionID, MessageID: "beat_" + connectionID + "_" + string(rune('0'+seq)),
				RequestMessageID: instance.HeartbeatMessageID(connectionID, seq), HeartbeatSeq: seq})
			if _, ackErr := response.Grant(); err != nil || ackErr != nil {
				t.Fatalf("heartbeat %d of %s = %d %s, %v; want its grant", seq, connectionID, response.Status, response.Body, errors.Join(err, ackErr))
			}
		}

		s := f.session()
		s.idle()
		team := f.createDrive()
		s.bind(team, "read_write")
		moved := s.preparedWithDrives("moved", everyTool, []protocol.DriveBinding{rw(team)})
		kept := f.connect(on(1, f.userInstance("kept")), everyTool)
		s.running()
		across, _ := s.admitted(moved.computerID, "sleep 60")
		first := moved.stream.executeOf(across)
		before := f.row(`SELECT lease_expires_at::text AS lease, hub_pod FROM instance_connections WHERE id=$1`, moved.stream.connectionID)

		f.hubProcesses[0].kill(t)
		for ended := false; !ended; {
			select {
			case _, open := <-moved.stream.events:
				ended = !open
			case <-time.After(10 * time.Second):
				t.Fatal("the stream on the killed process did not end")
			}
		}
		cli := on(1, moved.stream.cli)
		online("after the kill", cli, moved.stream.connectionID)
		meanwhile, _ := s.admitted(moved.computerID, "ls")
		beat(kept.cli, kept.connectionID, 1)
		kept.quiet(300 * time.Millisecond)

		resumed := f.resume(1, cli, moved.stream.connectionID)
		if connected := resumed.connected.Envelope.Payload.(protocol.ConnectedPayload); !connected.Resumed {
			t.Fatalf("connected = %s; want resumed", resumed.connected.Data)
		}
		after := f.row(`SELECT lease_expires_at::text AS lease, hub_pod FROM instance_connections WHERE id=$1`, moved.stream.connectionID)
		if after["lease"] != before["lease"] || before["hub_pod"] != "computer-hub-0" || after["hub_pod"] != "computer-hub-1" {
			t.Fatalf("connection %v after %v; want the same lease, moved from computer-hub-0 to computer-hub-1", after, before)
		}
		online("after the resume", cli, moved.stream.connectionID)
		if again := resumed.executeOf(across); again.Envelope.MessageID != first.Envelope.MessageID {
			t.Fatalf("first instruction after the resume = %s; want %s written again", again.Data, first.Envelope.MessageID)
		}
		second := resumed.executeOf(meanwhile)
		beat(cli, moved.stream.connectionID, 1)
		online("after the heartbeat", cli, moved.stream.connectionID)

		resumed.finish(first, "report_across")
		resumed.finish(second, "report_meanwhile")
		for _, use := range []string{across, meanwhile} {
			if state, record := s.settlement(use); state != "terminal_unconsumed" || record["settlement"] != "report" || record["phase"] != "terminal" {
				t.Fatalf("call %s = %s %v; want settled by its terminal report", use, state, record)
			}
		}

		seq := f.apiCommit(team, "v3.txt")
		later, _ := s.admitted(moved.computerID, "cat v3.txt")
		resumed.driveChanged(team, seq)
		resumed.executeOf(later)
		kept.quiet(300 * time.Millisecond)

		f.hubProcesses[1].terminate(t, 20*time.Second)
		t.Log("computer_hub_process_assertion=resumed_after_kill")
	})
}
