// Layer 2: the Engine-direction cases of protocol/behaviour-v1.cases.json
// against computer-hub with real PostgreSQL, through the test CLI.
package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/enginetest"
	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	computerhub "github.com/tetral-ai/tetral/services/computer-hub"
)

// TestBehaviourCases runs every Engine-direction case of the case file. A
// case, state, stimulus or step this runner does not implement fails it.
func TestBehaviourCases(t *testing.T) {
	file, err := enginetest.LoadBehaviourCases()
	if err != nil {
		t.Fatal(err)
	}
	for name := range file.States {
		if _, found := behaviourStates[name]; !found {
			t.Errorf("state %s is not implemented", name)
		}
	}
	for name := range file.Stimuli {
		if _, found := behaviourStimuli[name]; !found {
			t.Errorf("stimulus %s is not implemented", name)
		}
	}
	for _, c := range file.Cases {
		if c.Direction != "engine" {
			continue
		}
		t.Run(c.Name, func(t *testing.T) {
			run := &caseRun{t: t, clock: &caseClock{}, bindings: enginetest.NewBindings(file.Contents),
				sessions: map[string]*toolSession{}, bound: map[string]*caseBinding{}, credentials: map[string]*enginetest.CLI{}}
			run.f = newMachineTools(t, append(caseOptions(t, c), func(f *machineTools) { f.streamClock = run.clock })...)
			for _, given := range c.Given {
				behaviourStates[given.State](run, given.With)
			}
			for i, step := range c.Steps {
				run.step = i + 1
				switch {
				case step.Request != nil:
					run.request(*step.Request, *step.Answer)
				case step.Expect != nil:
					run.expect(*step.Expect)
				default:
					with, err := run.bindings.Expand(map[string]any(step.With))
					if err != nil {
						run.fatalf("%v", err)
					}
					behaviourStimuli[step.Stimulus](run, with.(map[string]any))
				}
			}
		})
	}
}

// caseOptions is the fixture options a case's states need before the hubs
// start.
func caseOptions(t *testing.T, c enginetest.Case) []machineToolsOption {
	var options []machineToolsOption
	for _, given := range c.Given {
		if given.State == "upload_budget" {
			perMinute, err := given.With["bytes_per_minute"].(json.Number).Int64()
			if err != nil {
				t.Fatal(err)
			}
			options = append(options, withUploadBudget(perMinute))
		}
		if version, set := given.With["min_cli_version"].(string); set && given.State == "instance" {
			options = append(options, func(f *machineTools) { f.minCLIVersion = version })
		}
	}
	return options
}

// caseRun is one case's run: its fixture, what it has bound, its instance's
// CLI and its current stream.
type caseRun struct {
	t        *testing.T
	f        *machineTools
	bindings *enginetest.Bindings
	step     int
	cli      *enginetest.CLI
	stream   *cliStream
	sessions map[string]*toolSession
	bound    map[string]*caseBinding
	// credentials holds the instance's CLI under each credential symbol.
	credentials map[string]*enginetest.CLI
	clock       *caseClock
}

// caseClock is the hubs' stream clock in a case: a timer fires when the
// case lets its time pass.
type caseClock struct {
	mu     sync.Mutex
	timers []caseTimer
}

type caseTimer struct {
	lasts time.Duration
	fire  chan time.Time
}

func (c *caseClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	fire := make(chan time.Time, 1)
	c.timers = append(c.timers, caseTimer{lasts: d, fire: fire})
	return fire
}

// pass fires the timers of at least from and less than to, once a stream
// has set one.
func (c *caseClock) pass(r *caseRun, from, to time.Duration) {
	r.t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		c.mu.Lock()
		fired := 0
		c.timers = slices.DeleteFunc(c.timers, func(timer caseTimer) bool {
			if timer.lasts < from || timer.lasts >= to {
				return false
			}
			timer.fire <- time.Time{}
			fired++
			return true
		})
		c.mu.Unlock()
		if fired > 0 {
			return
		}
		if time.Now().After(deadline) {
			r.fatalf("no stream set a timer of %v to %v", from, to)
		}
	}
}

// caseBinding is what a binding symbol stands for, also before its identity
// is known: its Session and its computer.
type caseBinding struct {
	session  *toolSession
	computer string
}

func (r *caseRun) fatalf(format string, args ...any) {
	r.t.Helper()
	r.t.Fatalf("step %d: %s", r.step, fmt.Sprintf(format, args...))
}

// text returns member name of with, a string, with its references
// replaced.
func (r *caseRun) text(with map[string]any, name string) string {
	r.t.Helper()
	value, err := r.bindings.WithString(with, name)
	if err != nil {
		r.fatalf("%v", err)
	}
	return value
}

// number returns member name of with, an integer.
func (r *caseRun) number(with map[string]any, name string) int64 {
	r.t.Helper()
	number, _ := with[name].(json.Number)
	value, err := number.Int64()
	if err != nil {
		r.fatalf("%q is no integer: %v", name, with[name])
	}
	return value
}

// symbol returns member name of with, a symbol.
func (r *caseRun) symbol(with map[string]any, name string) string {
	r.t.Helper()
	symbol, ok := with[name].(string)
	if !ok || symbol == "" {
		r.fatalf("%q names no symbol: %v", name, with[name])
	}
	return symbol
}

// drive returns the drive of symbol, created at its first use.
func (r *caseRun) drive(symbol string) string {
	r.t.Helper()
	if id, bound := r.bindings.Identity("drive", symbol); bound {
		return id
	}
	id := r.f.createDrive()
	r.bindings.Bind("drive", symbol, id)
	return id
}

// session returns the Session of symbol, created at its first use.
func (r *caseRun) session(symbol string) *toolSession {
	if s, found := r.sessions[symbol]; found {
		return s
	}
	s := r.f.session()
	s.idle()
	r.sessions[symbol] = s
	r.bindings.Bind("session", symbol, s.id)
	return s
}

var behaviourStates = map[string]func(*caseRun, map[string]any){
	"instance": func(r *caseRun, with map[string]any) {
		r.cli = r.f.userInstance(r.symbol(with, "instance"))
		r.bindings.Bind("instance", r.symbol(with, "instance"), r.cli.InstanceID)
		if symbol, named := with["credential"].(string); named {
			r.bindings.Bind("credential", symbol, r.cli.CredentialID)
			r.credentials[symbol] = r.cli
		}
	},
	"live_connection": func(r *caseRun, with map[string]any) {
		capabilities := protocol.Capabilities{}
		if tools, _ := with["tools"].(bool); tools {
			capabilities = everyTool
		}
		capabilities.Drives.Mount, _ = with["mount"].(bool)
		r.stream = r.f.connect(r.cli, capabilities)
		r.bindings.Connection = r.stream.connectionID
	},
	"prepared_binding": func(r *caseRun, with map[string]any) {
		r.prepares(with)
		prepare := r.stream.next()
		payload, ok := prepare.Envelope.Payload.(protocol.SessionPreparePayload)
		if !ok {
			r.t.Fatalf("event = %s; want session.prepare", prepare.Data)
		}
		r.stream.report(prepare, "")
		r.bindings.Bind("binding", r.symbol(with, "binding"), payload.BindingID)
	},
	"drive_file": func(r *caseRun, with map[string]any) { r.otherWriter(with) },
	"drive_deleted_file": func(r *caseRun, with map[string]any) {
		hello := "hello"
		r.write(with, enginetest.Content{Text: &hello}.Bytes())
		r.otherDeleter(with)
	},
	"drive_limits": func(r *caseRun, with map[string]any) {
		limits, err := json.Marshal(map[string]any{"limits": map[string]any{"max_bytes": with["max_bytes"], "max_files": with["max_files"]}})
		if err != nil {
			r.t.Fatal(err)
		}
		id := r.drive(r.symbol(with, "drive"))
		if status, body := r.f.developer(http.MethodPost, "/v1/drives/"+id, string(limits)); status != http.StatusOK {
			r.t.Fatalf("limits of %s = %d %s", id, status, body)
		}
	},
	// caseOptions gives the budget to the hubs before they start.
	"upload_budget": func(*caseRun, map[string]any) {},
}

var behaviourStimuli = map[string]func(*caseRun, map[string]any){
	"other_writer_commits": func(r *caseRun, with map[string]any) { r.otherWriter(with) },
	// Engine lists a record for 30 days by the database clock.
	"records_expire": func(r *caseRun, with map[string]any) {
		r.f.exec(`UPDATE drive_changes SET created_at=now()-interval '31 days' WHERE drive_id=$1 AND seq<$2`, r.drive(r.symbol(with, "drive")), r.number(with, "before_seq"))
	},
	// A commit removes the versions past the ten old ones it keeps of a
	// path with this statement's effect.
	"version_is_removed": func(r *caseRun, with map[string]any) {
		id, path, seq := r.drive(r.symbol(with, "drive")), r.text(with, "path"), r.number(with, "seq")
		const version = ` FROM drive_versions WHERE drive_id=$1 AND path=$2 AND seq=$3 AND superseded_at IS NOT NULL`
		if n := r.f.count(`SELECT count(*)`+version, id, path, seq); n != 1 {
			r.fatalf("%d replaced versions of %s at seq %d; want 1", n, path, seq)
		}
		r.f.exec(`DELETE`+version, id, path, seq)
	},
	"engine_prepares": func(r *caseRun, with map[string]any) { r.prepares(with) },
	"engine_closes": func(r *caseRun, with map[string]any) {
		binding := r.binding(with)
		if status, object := binding.session.remove(binding.computer); status != http.StatusAccepted {
			r.fatalf("removing the binding's computer = %d %v; want 202", status, object)
		}
	},
	"engine_executes": func(r *caseRun, with map[string]any) {
		binding := r.binding(with)
		binding.session.running()
		binding.session.admitted(binding.computer, "true")
	},
	"engine_changes_drives": func(r *caseRun, with map[string]any) {
		binding, id := r.binding(with), r.drive(r.symbol(with, "drive"))
		binding.session.idle()
		access, kept := with["access"].(string)
		held := false
		for _, drive := range r.f.sessionDrives(binding.session.id) {
			held = held || drive.DriveID == id
		}
		switch {
		case !kept:
			binding.session.unbind(id)
		case held:
			binding.session.rebind(id, access)
		default:
			binding.session.bind(id, access)
		}
	},
	"stream_ends": func(r *caseRun, _ map[string]any) { r.stream.close() },
	"heartbeat_is_due": func(r *caseRun, _ map[string]any) {
		r.clock.pass(r, computerhub.HeartbeatInterval, computerhub.HeartbeatInterval+1)
	},
	"stream_reaches_its_maximum_age": func(r *caseRun, _ map[string]any) {
		r.clock.pass(r, computerhub.DefaultStreamMaxAge, 2*computerhub.DefaultStreamMaxAge)
	},
	// Engine measures a lease by the database clock.
	"lease_ends": func(r *caseRun, _ map[string]any) {
		r.f.exec(`UPDATE instance_connections SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, r.bindings.Connection)
	},
	"engine_renews_the_credential": func(r *caseRun, with map[string]any) {
		symbol := r.symbol(with, "credential")
		status, body, err := r.f.controllerKeyed(0, http.MethodPost, "/v1/instances/"+r.cli.InstanceID+"/credentials", `{"delivery":"stream"}`, "renewal-"+symbol)
		var issued struct {
			ID string `json:"id"`
		}
		if err != nil || status != http.StatusAccepted || json.Unmarshal(body, &issued) != nil || issued.ID == "" {
			r.fatalf("a credential for the stream = %d %s", status, body)
		}
		r.bindings.Bind("credential", symbol, issued.ID)
		r.credentials[symbol] = r.f.cli(0, r.cli.InstanceID, issued.ID)
	},
	"credential_revoked": func(r *caseRun, with map[string]any) {
		r.f.ownerTx(func(tx *dbconnect.Tx) error {
			_, err := instance.RevokeCredential(r.f.ctx, tx, r.f.ws, r.cli.InstanceID, r.credential(with).CredentialID)
			return err
		})
	},
	// Engine measures a credential's expiry by the database clock.
	"credential_expired": func(r *caseRun, with map[string]any) {
		r.f.exec(`UPDATE instance_credentials SET issued_at=clock_timestamp()-interval '1 hour',expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`,
			r.credential(with).CredentialID)
	},
}

// credential returns the instance's CLI under the credential symbol of
// with.
func (r *caseRun) credential(with map[string]any) *enginetest.CLI {
	r.t.Helper()
	cli, found := r.credentials[r.symbol(with, "credential")]
	if !found {
		r.fatalf("credential %v is not the case's", with["credential"])
	}
	return cli
}

// prepares issues a session.prepare for the Session of with on the case's
// connection, with its drives bound to the Session first.
func (r *caseRun) prepares(with map[string]any) {
	r.t.Helper()
	s := r.session(r.symbol(with, "session"))
	entries, _ := with["drives"].([]any)
	for _, entry := range entries {
		binding := entry.(map[string]any)
		s.bind(r.drive(r.symbol(binding, "drive")), r.text(binding, "access"))
	}
	computerID := s.userComputer(r.cli)
	r.f.ownerTx(func(tx *dbconnect.Tx) error {
		_, err := computer.IssuePreparation(r.f.ctx, tx, r.f.ws, s.id, computerID)
		return err
	})
	r.bound[r.symbol(with, "binding")] = &caseBinding{session: s, computer: computerID}
}

// binding returns what the binding symbol of with stands for.
func (r *caseRun) binding(with map[string]any) *caseBinding {
	r.t.Helper()
	binding, found := r.bound[r.symbol(with, "binding")]
	if !found {
		r.fatalf("binding %v was not prepared", with["binding"])
	}
	return binding
}

// otherWriter commits one change of a drive's path through the developers'
// API: content written over whatever the path holds.
func (r *caseRun) otherWriter(with map[string]any) {
	r.t.Helper()
	content, err := r.bindings.Content(r.symbol(with, "content"))
	if err != nil {
		r.fatalf("%v", err)
	}
	r.write(with, content)
}

func (r *caseRun) write(with map[string]any, content []byte) {
	r.t.Helper()
	id, path := r.drive(r.symbol(with, "drive")), r.text(with, "path")
	if status, body := r.f.apiWrite(id, path, content, r.f.driveFiles(id)[path]); status != http.StatusOK && status != http.StatusCreated {
		r.fatalf("another writer's write of %s = %d %s", path, status, body)
	}
}

// otherDeleter deletes a drive's path through the developers' API.
func (r *caseRun) otherDeleter(with map[string]any) {
	r.t.Helper()
	id, path := r.drive(r.symbol(with, "drive")), r.text(with, "path")
	target := "/v1/drives/" + id + "/files?path=" + url.QueryEscape(path) + "&expected_content_sha256=" + r.f.driveFiles(id)[path]
	if status, body := r.f.developer(http.MethodDelete, target, ""); status != http.StatusOK && status != http.StatusNoContent {
		r.fatalf("another writer's deletion of %s = %d %s", path, status, body)
	}
}

// request sends a case's request through the test CLI and holds its answer
// to the case.
func (r *caseRun) request(request enginetest.CaseRequest, answer enginetest.CaseAnswer) {
	r.t.Helper()
	path, err := r.bindings.ExpandString(request.Path)
	if err != nil {
		r.fatalf("%v", err)
	}
	query := url.Values{}
	for name, value := range request.Query {
		expanded, err := r.bindings.ExpandString(value)
		if err != nil {
			r.fatalf("%v", err)
		}
		query.Set(name, expanded)
	}
	header := http.Header{}
	for name, value := range request.Headers {
		header.Set(name, value)
	}
	var body []byte
	switch {
	case request.Content != "":
		if body, err = r.bindings.Content(request.Content); err != nil {
			r.fatalf("%v", err)
		}
	case request.Body != nil:
		expanded, err := r.bindings.Expand(request.Body)
		if err != nil {
			r.fatalf("%v", err)
		}
		if body, err = json.Marshal(expanded); err != nil {
			r.fatalf("%v", err)
		}
		header.Set("Content-Type", "application/json")
	}
	cli := r.cli
	if request.Credential != "" {
		cli = r.credential(map[string]any{"credential": request.Credential})
	}
	sender := cli
	if request.Invalid {
		sender = cli.Invalid()
	}
	if request.Method == http.MethodGet && path == "/v1/client/events" {
		r.open(sender, cli, query, answer)
		return
	}
	cli = sender
	target := path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	var response enginetest.Response
	if request.DeclaresBytes != nil {
		response = r.unsent(cli, request.Method, target, header, *request.DeclaresBytes)
	} else if response, err = cli.Request(r.f.ctx, request.Method, target, header, body); err != nil {
		r.fatalf("%s %s: %v", request.Method, path, err)
	}
	r.answered(request.Method+" "+path, response, answer)
}

// unsent sends a request that declares length body bytes and sends none;
// its answer must come before the body.
func (r *caseRun) unsent(cli *enginetest.CLI, method, target string, header http.Header, length int64) enginetest.Response {
	r.t.Helper()
	body := blockedBody{release: make(chan struct{})}
	defer close(body.release)
	response, err := cli.Open(r.f.ctx, method, target, header, body, length)
	if err != nil {
		r.fatalf("%s declaring %d bytes: %v; want an answer before the body", method, length, err)
	}
	defer func() { _ = response.Body.Close() }()
	data, _ := io.ReadAll(response.Body)
	return enginetest.Response{Status: response.StatusCode, Header: response.Header, Body: data}
}

// open sends an events request; a 200 answer makes its stream the case's.
func (r *caseRun) open(sender, cli *enginetest.CLI, query url.Values, answer enginetest.CaseAnswer) {
	r.t.Helper()
	stream, response, err := sender.OpenEvents(r.f.ctx, query)
	if err != nil {
		r.fatalf("open events: %v", err)
	}
	if stream == nil {
		r.answered("GET /v1/client/events", response, answer)
		return
	}
	if answer.Status != http.StatusOK {
		r.fatalf("GET /v1/client/events = 200; want %d", answer.Status)
	}
	s := &cliStream{f: r.f, cli: cli, stream: stream, events: make(chan enginetest.Event, 64)}
	r.t.Cleanup(func() { _ = stream.Close() })
	go func() {
		defer close(s.events)
		for {
			event, err := stream.Next()
			if err != nil {
				return
			}
			s.events <- event
		}
	}()
	r.stream = s
}

func (r *caseRun) answered(what string, response enginetest.Response, answer enginetest.CaseAnswer) {
	r.t.Helper()
	if response.Status != answer.Status {
		r.fatalf("%s = %d %s; want %d", what, response.Status, response.Body, answer.Status)
	}
	for name, want := range answer.Headers {
		if err := r.bindings.Match(want, response.Header.Get(name)); err != nil {
			r.fatalf("%s: header %s%v", what, name, err)
		}
	}
	switch {
	case answer.Content != nil:
		content, err := r.bindings.Content(answer.Content.Of)
		if err != nil {
			r.fatalf("%v", err)
		}
		if answer.Content.From != nil {
			content = content[*answer.Content.From : *answer.Content.To+1]
		}
		if !bytes.Equal(response.Body, content) {
			r.fatalf("%s: body is %d bytes that differ from the %d expected", what, len(response.Body), len(content))
		}
	case answer.Body != nil:
		got, err := enginetest.DecodeJSON(response.Body)
		if err != nil {
			r.fatalf("%s: body %s: %v", what, brief(response.Body), err)
		}
		if err := r.bindings.Match(answer.Body, got); err != nil {
			r.fatalf("%s: body %s: %v", what, brief(response.Body), err)
		}
	}
}

// brief is data for a failure message, without the middle of a long one.
func brief(data []byte) string {
	if len(data) <= 1200 {
		return string(data)
	}
	return fmt.Sprintf("%s … %s (%d bytes)", data[:600], data[len(data)-600:], len(data))
}

// expect holds the next event of the case's stream, or its end, to the
// case.
func (r *caseRun) expect(expectation enginetest.Expectation) {
	r.t.Helper()
	if expectation.Request != nil {
		r.fatalf("an Engine-direction case expects no request")
	}
	want, _ := expectation.Event.(map[string]any)
	timeout := time.After(20 * time.Second)
	for {
		select {
		case event, open := <-r.stream.events:
			if !open {
				if !expectation.End {
					r.fatalf("the stream ended; want %v", expectation.Event)
				}
				return
			}
			_, beat := event.Envelope.Payload.(protocol.HeartbeatPayload)
			if beat && (expectation.End || want["type"] != "heartbeat") {
				continue
			}
			if expectation.End {
				r.fatalf("event %s; want the stream's end", event.Data)
			}
			got, err := enginetest.DecodeJSON(event.Data)
			if err != nil {
				r.fatalf("event %s: %v", event.Data, err)
			}
			if err := r.bindings.Match(expectation.Event, got); err != nil {
				r.fatalf("event %s: %v", event.Data, err)
			}
			if _, connected := event.Envelope.Payload.(protocol.ConnectedPayload); connected {
				r.bindings.Connection = event.Envelope.ConnectionID
				r.stream.connectionID = event.Envelope.ConnectionID
			}
			return
		case <-timeout:
			r.fatalf("no event within 20 seconds; want %v", expectation)
		}
	}
}
