package enginetest

import (
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/tetral-ai/tetral/internal/protocol"
)

// machineStub serves a stream that sends connected, one heartbeat and one
// session.prepare, acknowledges every report and records each request's
// path.
type machineStub struct {
	mu    sync.Mutex
	paths []string
}

func (s *machineStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/client/events" {
		s.mu.Lock()
		s.paths = append(s.paths, r.URL.Path)
		s.mu.Unlock()
		acknowledge(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	_, _ = io.WriteString(w, connected+event("msg_hb_1", "heartbeat", `{"heartbeat_seq":1}`)+event("msg_prepare", "session.prepare", prepare))
	w.(http.Flusher).Flush()
	<-r.Context().Done()
}

func (s *machineStub) sent() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.paths, " ")
}

func stubMachine(t *testing.T) (*Machine, *machineStub) {
	stub := &machineStub{}
	server := httptest.NewServer(stub)
	t.Cleanup(server.Close)
	cli := NewCLI(t)
	cli.BaseURL, cli.InstanceID, cli.CredentialID, cli.Key = server.URL, "inst_example", "ccred_example", "tetral_ck_"+strings.Repeat("Q", 43)
	machine := NewMachine(cli, protocol.Capabilities{})
	t.Cleanup(machine.Close)
	return machine, stub
}

// The default script sends ready and answers the heartbeat, and a Session
// report goes out under its case; a replaced step is the test's own.
func TestTheMachineRunsItsScriptAndLetsATestReplaceAStep(t *testing.T) {
	machine, stub := stubMachine(t)
	if err := machine.Connect(t.Context()); err != nil {
		t.Fatal(err)
	}
	instruction := <-machine.Events
	if _, ok := instruction.Envelope.Payload.(protocol.SessionPreparePayload); !ok {
		t.Fatalf("event %s; want the session.prepare, the heartbeat consumed", instruction.Data)
	}
	if answer, err := machine.Report(instruction); err != nil || answer.Status != http.StatusOK {
		t.Fatalf("prepared report = %d, %v", answer.Status, err)
	}
	if got, want := stub.sent(), "/v1/client/ready /v1/client/heartbeat /v1/client/session-status"; got != want {
		t.Fatalf("requests %q; want %q", got, want)
	}

	silent, stub := stubMachine(t)
	silent.Heartbeat = nil
	if err := silent.Connect(t.Context()); err != nil {
		t.Fatal(err)
	}
	beat := <-silent.Events
	if _, ok := beat.Envelope.Payload.(protocol.HeartbeatPayload); !ok {
		t.Fatalf("event %s; want the heartbeat handed to the test", beat.Data)
	}
	if got := stub.sent(); got != "/v1/client/ready" {
		t.Fatalf("requests %q; want the ready alone", got)
	}
}

// A report that is not what its case states as the CLI's reaction is
// refused before it is sent.
func TestAReactionIsHeldToItsCase(t *testing.T) {
	machine, stub := stubMachine(t)
	if err := machine.Connect(t.Context()); err != nil {
		t.Fatal(err)
	}
	instruction := <-machine.Events
	report := func(change func(*protocol.SessionStatus)) []byte {
		status := SessionReport(machine.ConnectionID, instruction, "prepared")
		change(&status)
		return protocol.Encode(status)
	}
	asIs := func(*protocol.SessionStatus) {}
	for _, row := range []struct {
		name, held string
		reports    [][]byte
		refusal    string
	}{
		{"the case's report", CasePrepared, [][]byte{report(asIs)}, ""},
		{"another status", CasePrepared, [][]byte{report(func(s *protocol.SessionStatus) { s.Status = "closed" })}, "/status"},
		{"another instruction's id", CasePrepared, [][]byte{report(func(s *protocol.SessionStatus) { s.RequestMessageID = "msg_another" })}, "/request_message_id"},
		{"another binding", CasePrepared, [][]byte{report(func(s *protocol.SessionStatus) { s.BindingID = "bind_another" })}, "/binding_id"},
		{"two reports where the case has one", CasePrepared, [][]byte{report(asIs), report(asIs)}, "has 1 requests"},
		{"no report", CasePrepared, nil, "has 1 requests"},
		{"an instruction that is not the case's trigger", CaseClosed, [][]byte{report(asIs)}, "is not its trigger"},
		{"an Engine-direction case", "a_ready_is_accepted_once", [][]byte{report(asIs)}, "no CLI-direction case"},
	} {
		before := stub.sent()
		_, err := machine.React(row.held, instruction, row.reports...)
		switch {
		case row.refusal == "" && err != nil:
			t.Errorf("%s: %v; want it sent", row.name, err)
		case row.refusal != "" && (err == nil || !strings.Contains(err.Error(), row.refusal)):
			t.Errorf("%s: %v; want a refusal naming %q", row.name, err, row.refusal)
		case row.refusal != "" && stub.sent() != before:
			t.Errorf("%s: the refused report was sent", row.name)
		}
	}
}

// A CLI-direction case exists for a report sequence Engine's tests send as
// the CLI's; the cases of the file are exactly the ones the machine names.
func TestTheMachineNamesEveryCLIDirectionCase(t *testing.T) {
	file, err := LoadBehaviourCases()
	if err != nil {
		t.Fatal(err)
	}
	named := []string{CaseReady, CaseHeartbeat, CasePrepared, CaseClosed, CaseApplied, CaseEnded}
	slices.Sort(named)
	inFile := file.Direction("cli")
	slices.Sort(inFile)
	if !slices.Equal(named, inFile) {
		t.Fatalf("the machine names %q; the case file has %q", named, inFile)
	}
}
