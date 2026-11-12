package integration

import (
	"context"
	"encoding/hex"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/enginetest"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

// hubProcess is a hub server running as the built tetral-computer-hub
// command.
type hubProcess struct {
	pod     string
	command *exec.Cmd
	output  lockedBuffer
	joined  chan error
	// ended is set once the test itself has ended the process.
	ended bool
}

// withHubProcesses starts each hub server as the built command instead of
// an in-process server. The command opens its database as production does,
// so the test runs under isolatedTLSPostgreSQLRoot.
func withHubProcesses(t *testing.T) machineToolsOption {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "tetral-computer-hub")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if output, err := buildCommand(ctx, binary, "./services/computer-hub/cmd/tetral-computer-hub").CombinedOutput(); err != nil {
		t.Fatalf("build tetral-computer-hub: %v: %s", err, output)
	}
	return func(f *machineTools) { f.hubBinary = binary }
}

// startHubProcess starts the built command as pod and returns its URL once
// it is ready. A process that exits before the test ends it fails the test.
func (f *machineTools) startHubProcess(pod string) string {
	f.t.Helper()
	addresses, release := publicReserveAddresses(f.t.(*testing.T), 2)
	process := &hubProcess{pod: pod, joined: make(chan error, 1)}
	process.command = exec.Command(f.hubBinary) //nolint:gosec // The repository's own command, built by this test.
	process.command.Env = oidcChildEnvironment(map[string]string{
		"TETRAL_COMPUTER_HUB_HTTP_ADDR":                 addresses[0],
		"TETRAL_COMPUTER_HUB_METRICS_ADDR":              addresses[1],
		"TETRAL_HTTP_TRANSPORT":                         "plaintext",
		"TETRAL_DATABASE_URL":                           storagetest.RuntimeDatabaseURL(f.t, f.hubPool),
		"TETRAL_DATABASE_TLS_CA_PATH":                   filepath.Join(os.Getenv(envIsolatedTLSPGCerts), "ca.pem"),
		"TETRAL_DATABASE_TLS_SERVER_NAME":               "postgres.transport.test",
		"TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64": f.signer.PublicKeyBase64(),
		"TETRAL_COMPUTER_HUB_PAGE_TOKEN_KEY":            hex.EncodeToString(make([]byte, 32)),
		"TETRAL_COMPUTER_HUB_POD_NAME":                  pod,
		"TETRAL_COMPUTER_HUB_SERVER_URL":                "https://api.tetral.test",
		"TETRAL_COMPUTER_HUB_GIT_PROXY_URL":             "https://git.tetral.test",
		"TETRAL_COMPUTER_HUB_STREAM_DRAIN_WINDOW":       "1s",
		"TETRAL_DEPLOYMENT_ENVIRONMENT":                 "test",
		"TETRAL_SERVICE_VERSION":                        "process-fixture",
		// The command builds its protected blob client without a request,
		// and no test of these processes transfers content: the store is an
		// address nothing listens on, trusted by a certificate at hand.
		"TETRAL_BLOB_ENDPOINT":        "https://127.0.0.1:9",
		"TETRAL_BLOB_REGION":          "us-east-1",
		"TETRAL_BLOB_BUCKET":          "unused",
		"TETRAL_BLOB_ACCESS_KEY":      "unused",
		"TETRAL_BLOB_SECRET_KEY":      "unused-secret",
		"TETRAL_BLOB_ALLOW_INSECURE":  "false",
		"TETRAL_BLOB_LOCAL_TEST_MODE": "false",
		"TETRAL_BLOB_TLS_CA_PATH":     filepath.Join(os.Getenv(envIsolatedTLSPGCerts), "ca.pem"),
		"TETRAL_BLOB_TLS_SERVER_NAME": "blob.transport.test",
	})
	process.command.Stdout, process.command.Stderr = &process.output, &process.output
	release()
	if err := process.command.Start(); err != nil {
		f.t.Fatal(err)
	}
	go func() { process.joined <- process.command.Wait() }()
	f.hubProcesses = append(f.hubProcesses, process)
	f.t.Cleanup(func() {
		if !process.ended {
			process.terminate(f.t, 20*time.Second)
		}
	})
	url, client := "http://"+addresses[0], &http.Client{Timeout: time.Second}
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		select {
		case err := <-process.joined:
			process.ended = true
			f.t.Fatalf("%s exited at its start: %v: %s", pod, err, process.output.String())
		default:
		}
		if response, err := client.Get(url + "/ready"); err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return url
			}
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("%s was not ready within 30 seconds: %s", pod, process.output.String())
		}
	}
}

// kill ends the process with SIGKILL: nothing of it runs afterwards.
func (p *hubProcess) kill(t testing.TB) {
	t.Helper()
	p.ended = true
	if err := p.command.Process.Kill(); err != nil {
		t.Fatalf("kill %s: %v", p.pod, err)
	}
	<-p.joined
}

// terminate sends SIGTERM; the process must exit 0 within limit.
func (p *hubProcess) terminate(t testing.TB, limit time.Duration) {
	t.Helper()
	p.ended = true
	if err := p.command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Errorf("%s had exited on its own before its SIGTERM: %v: %s", p.pod, err, p.output.String())
		return
	}
	select {
	case err := <-p.joined:
		if err != nil {
			t.Errorf("%s after SIGTERM: %v: %s", p.pod, err, p.output.String())
		}
	case <-time.After(limit):
		_ = p.command.Process.Kill()
		<-p.joined
		t.Errorf("%s did not exit within %s of SIGTERM", p.pod, limit)
	}
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
