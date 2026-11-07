package joint

import (
	"bytes"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/protocol"
)

// scenario starts the Engine with options and the machines of a scenario.
func scenario(t *testing.T, options engineOptions) (*engine, *machines, *acceptanceRecord) {
	t.Helper()
	return scenarioWith(t, options, needs{})
}

// scenarioWith is scenario for a test that needs n.
func scenarioWith(t *testing.T, options engineOptions, n needs) (*engine, *machines, *acceptanceRecord) {
	t.Helper()
	in := requireInputs(t, n)
	record := newAcceptanceRecord(t, in, platformOf(in))
	e := startEngine(t, record, options)
	return e, newMachines(t, in, n, e, record), record
}

// scenarioRow is one row of a table test: a scenario that runs on the
// table's Engine with machines and a record section of its own.
type scenarioRow struct {
	name string
	run  func(t *testing.T, e *engine, set *machines, record *acceptanceRecord)
	// platforms are the machine platforms the row runs on, every platform
	// when empty.
	platforms []string
}

// tableRow is the row name that run runs on every platform.
func tableRow(name string, run func(t *testing.T, e *engine, set *machines, record *acceptanceRecord)) scenarioRow {
	return scenarioRow{name: name, run: run}
}

// only is the row, run only on platforms and left out, with no record, on
// the others.
func (r scenarioRow) only(platforms ...string) scenarioRow {
	r.platforms = platforms
	return r
}

// scenarioTable starts one Engine with options and its Runtime for rows that
// need n, and runs each row of the machines' platform as a subtest on it,
// with a record of its own. What a row puts in place at the edge ends with
// the row. A killed computer-hub pod stays down, so a row that kills one
// comes after the rows that need every pod.
func scenarioTable(t *testing.T, options engineOptions, n needs, rows ...scenarioRow) {
	t.Helper()
	e, set, record := scenarioWith(t, options, n)
	record.table = true
	e.startRuntime()
	for _, row := range rows {
		if len(row.platforms) > 0 && !slices.Contains(row.platforms, set.in.machineOS) {
			continue
		}
		t.Run(row.name, func(t *testing.T) {
			t.Cleanup(e.edge.scope())
			rowRecord := record.row(t, row.name)
			rowEngine := e.forRow(t, rowRecord)
			row.run(t, rowEngine, newMachines(t, set.in, n, rowEngine, rowRecord), rowRecord)
		})
	}
}

// userMachine is a CLI machine online on its own user instance.
type userMachine struct {
	*machine
	instanceID   string
	credentialID string
	server       *cliProcess
}

// onlineUserMachine registers a user instance, configures the machine name
// with it, starts its App Server with args and waits until it is online.
func onlineUserMachine(t *testing.T, e *engine, set *machines, name string, args ...string) *userMachine {
	t.Helper()
	return onlineUserMachineOf(t, e.controller(), e, set, name, args...)
}

// onlineUserMachineOf is onlineUserMachine with an instance that controller
// registers in its workspace.
func onlineUserMachineOf(t *testing.T, controller *controllerClient, e *engine, set *machines, name string, args ...string) *userMachine {
	t.Helper()
	return onlineUserMachineFor(t, controller, e, set.add(name, name+"-app"), args...)
}

// onlineUserMachineFor is onlineUserMachineOf for the started machine m.
func onlineUserMachineFor(t *testing.T, controller *controllerClient, e *engine, m *machine, args ...string) *userMachine {
	t.Helper()
	registered := controller.createUserInstance(t.Context(), m.name)
	m.configure(bootstrap(e.serverURL, registered.instance.ID, registered.credential))
	u := &userMachine{machine: m, instanceID: registered.instance.ID, credentialID: registered.credential.ID, server: m.serve(args...)}
	waitOnline(t, controller, m, registered.instance.ID, registered.credential.ID)
	return u
}

// restart stops the machine's App Server and starts it again.
func (u *userMachine) restart() {
	u.t.Helper()
	u.server.stop()
	u.server = u.serve()
}

// driveSession creates a Session that binds drives, whose computers are an
// unserved cloud computer and a user computer on each machine, in the
// directory name on that machine, which it creates. It waits until every
// user computer is ready, binds the Session to the test Runtime and returns
// the Session, the user computers' IDs in machine order and the bound
// Session.
func driveSession(t *testing.T, e *engine, name string, drives []sessionDrive, ms ...*userMachine) (sessionObject, []string, *runtimeSession) {
	t.Helper()
	return driveSessionWith(t, e, name, drives, nil, ms...)
}

// driveSessionWith is driveSession with fields of extras[i] added to the
// user computer of ms[i].
func driveSessionWith(t *testing.T, e *engine, name string, drives []sessionDrive, extras []map[string]any, ms ...*userMachine) (sessionObject, []string, *runtimeSession) {
	t.Helper()
	return driveSessionOf(t, e, e.developer(), harnessWorkspace, name, drives, extras, ms...)
}

// driveSessionOf is driveSessionWith for developer's workspace ws.
func driveSessionOf(t *testing.T, e *engine, developer *developerClient, ws, name string, drives []sessionDrive, extras []map[string]any, ms ...*userMachine) (sessionObject, []string, *runtimeSession) {
	t.Helper()
	ctx := t.Context()
	items := []map[string]any{developer.unservedCloudComputer(ctx, name)}
	for i, m := range ms {
		work := m.directory(name)
		if code, _ := m.shell("mkdir", "-p", work); code != 0 {
			t.Fatalf("mkdir %s on %s exited %d", work, m.name, code)
		}
		item := map[string]any{"instance_id": m.instanceID, "working_directory": work}
		if i < len(extras) {
			maps.Copy(item, extras[i])
		}
		items = append(items, item)
	}
	session := developer.createSessionWithDrives(ctx, developer.createAgent(ctx, name), drives, items...)
	var computers []string
	for i, m := range ms {
		computer := session.Computers[i+1]
		ready := developer.waitComputer(ctx, session.ID, computer.ID, time.Minute, "ready", func(c computerObject) bool { return c.ExecutionState == "ready" })
		e.record.observe("Session %s: user computer %s ready on %s with binding %s", session.ID, computer.ID, m.name, *ready.BindingID)
		computers = append(computers, computer.ID)
	}
	return session, computers, e.startRuntime().bindIn(ws, session.ID)
}

// inDrive is the shell word of path in driveID's mount, for a command's
// text.
func inDrive(driveID, path string) string {
	return `"$TETRAL_DRIVES/` + driveID + "/" + path + `"`
}

// rejectedChange is one entry of drive_sync.rejected.
type rejectedChange struct {
	Path   string `json:"path"`
	Change string `json:"change"`
	Reason string `json:"reason"`
	Rule   string `json:"rule"`
}

// driveSync is the drive_sync of a command's result.
type driveSync struct {
	Committed int64            `json:"committed"`
	Deleted   int64            `json:"deleted"`
	Rejected  []rejectedChange `json:"rejected"`
	NotSynced []struct {
		Path   string `json:"path"`
		Reason string `json:"reason"`
	} `json:"not_synced"`
	Downloading []struct {
		Path            string `json:"path"`
		DownloadedBytes int64  `json:"downloaded_bytes"`
		SizeBytes       int64  `json:"size_bytes"`
	} `json:"downloading"`
	Unmounted []struct {
		DriveID string `json:"drive_id"`
		Error   string `json:"error"`
	} `json:"unmounted"`
}

// lists reports whether s lists path of driveID's mount in not_synced with
// reason.
func (s *driveSync) lists(driveID, path, reason string) bool {
	return s != nil && slices.ContainsFunc(s.NotSynced, func(n struct {
		Path   string `json:"path"`
		Reason string `json:"reason"`
	}) bool {
		return strings.HasSuffix(n.Path, "/drives/"+driveID+"/"+path) && n.Reason == reason
	})
}

// run admits cmd to computerID, waits up to timeout for its terminal report
// and checks that it exited 0; it returns the trimmed stdout and the result
// envelope.
func (s *runtimeSession) run(computerID, cmd string, timeout time.Duration) (string, execEnvelope) {
	t := s.r.e.t
	t.Helper()
	call := s.exec(computerID, cmd)
	envelope := s.terminal(call, timeout)
	if envelope.Result.ExitCode == nil || *envelope.Result.ExitCode != 0 {
		t.Fatalf("call %s of %q result %+v; want exit 0", call.use, cmd, envelope)
	}
	return strings.TrimSpace(envelope.Result.Stdout.Text), envelope
}

// loseConnection ends the machine's connection: it kills the computer-hub
// pod its stream uses and holds its reconnects until the instance reads
// offline, so that the next connection is a new one. The engine needs
// another pod to take that connection.
func loseConnection(e *engine, m *userMachine) {
	e.t.Helper()
	hold := e.edge.holdEvents(m.instanceID)
	requests := e.edge.eventsRequests(m.instanceID)
	e.killHub(requests[len(requests)-1].backend)
	hold.waitArrived(e.t, time.Minute)
	m.waitOffline(e)
	e.edge.releaseEvents(m.instanceID)
}

// preparedAgain waits until the Session's computer is ready with a binding
// other than previous.
func preparedAgain(e *engine, sessionID, computerID, previous string) {
	e.t.Helper()
	e.developer().waitComputer(e.t.Context(), sessionID, computerID, 2*time.Minute, "prepared again", func(c computerObject) bool {
		return c.ExecutionState == "ready" && c.BindingID != nil && *c.BindingID != previous
	})
}

// runInBackground admits a command to computerID that runs until the
// Session ends and returns once its first report says it runs. While a
// command runs, the CLI uploads what a process outside the CLI writes into a
// mount; otherwise that waits for the next sync point.
func (s *runtimeSession) runInBackground(computerID string) {
	s.r.e.t.Helper()
	s.background(computerID, "sleep 86400")
}

// background admits cmd to computerID and returns once its first report
// says it runs.
func (s *runtimeSession) background(computerID, cmd string) {
	t := s.r.e.t
	t.Helper()
	call := s.execInput(computerID, map[string]any{"cmd": cmd, "wait_ms": 1000})
	settled := s.await(call.use, time.Minute)
	var envelope execEnvelope
	if settled.Settlement != "report" || json.Unmarshal(settled.Result, &envelope) != nil || envelope.Status != "running" {
		t.Fatalf("the background command %s settled %+v; want a running report", call.use, settled)
	}
}

// uploadOutside runs script on m in driveID's mount, outside the CLI, while a
// command runs (runInBackground), and waits until the edge has accepted the
// uploads of n more contents of the drive. The next sync point then finds
// every change uploaded and commits them together, whatever the CLI's
// upload rate. Each file the script writes needs content of its own, since
// equal content is uploaded once.
func uploadOutside(e *engine, m *userMachine, sessionID, driveID, script string, n int) {
	t := e.t
	t.Helper()
	accepted := func() int {
		contents := map[string]bool{}
		for _, r := range e.edge.answered(http.MethodPut, contentRoute(driveID)) {
			if r.status == http.StatusOK || r.status == http.StatusCreated {
				contents[r.path] = true
			}
		}
		return len(contents)
	}
	want := accepted() + n
	started := time.Now()
	if code, out := m.shellWithin(30*time.Minute, "sh", "-c", `cd "$1" && `+script, "sh", m.mountPoint(sessionID, driveID)); code != 0 {
		t.Fatalf("%q on %s exited %d: %s", script, m.name, code, out)
	}
	wrote := time.Since(started)
	deadline := time.Now().Add(20 * time.Minute)
	for accepted() < want {
		if time.Now().After(deadline) {
			t.Fatalf("the edge accepted %d of %d uploads within 20 minutes after %q", accepted()-want+n, n, script)
		}
		time.Sleep(time.Second)
	}
	e.record.observe("%q took %s on %s outside the CLI; its %d uploads were accepted %s later", script, wrote.Round(time.Second), m.name, n, (time.Since(started) - wrote).Round(time.Second))
}

// contentRoute and commitRoute match a drive's content and commit requests
// of the client routes.
func contentRoute(driveID string) *regexp.Regexp {
	return regexp.MustCompile(`^/v1/client/drives/` + driveID + `/content/[0-9a-f]{64}$`)
}

func commitRoute(driveID string) *regexp.Regexp {
	return regexp.MustCompile(`^/v1/client/drives/` + driveID + `/commits$`)
}

// refuseUploads answers every content PUT of driveID with 503, a temporary
// failure, until the returned function runs.
func refuseUploads(e *engine, driveID string) func() {
	route := contentRoute(driveID)
	return e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool { return r.Method == http.MethodPut && route.MatchString(r.URL.Path) },
		before: func(w http.ResponseWriter, _ *http.Request) bool {
			w.WriteHeader(http.StatusServiceUnavailable)
			return true
		},
	})
}

// waitOffline waits until the instance read shows its connection offline.
func (u *userMachine) waitOffline(e *engine) {
	u.t.Helper()
	e.controller().waitInstance(u.t.Context(), u.instanceID, time.Minute, "connection offline", func(object instanceObject) bool {
		return object.Connection != nil && object.Connection.State == "offline"
	})
}

// sessionData reports whether the machine's state root holds data of the
// Session.
func (u *userMachine) sessionData(sessionID string) bool {
	u.t.Helper()
	code, _ := u.shell("test", "-e", u.home+"/sessions/"+sessionID)
	return code == 0
}

// unisolatedMachine is a Linux machine online on its own user instance whose
// App Server finds no bwrap on its PATH, so that platform isolation is
// unavailable to its tool calls.
func unisolatedMachine(t *testing.T, e *engine, set *machines, name string) *userMachine {
	t.Helper()
	controller := e.controller()
	registered := controller.createUserInstance(t.Context(), name)
	m := set.add(name, name+"-app")
	empty := m.directory("empty-path")
	if code, _ := m.shell("mkdir", "-p", empty); code != 0 {
		t.Fatalf("mkdir %s exited %d", empty, code)
	}
	m.configure(bootstrap(e.serverURL, registered.instance.ID, registered.credential))
	u := &userMachine{machine: m, instanceID: registered.instance.ID, credentialID: registered.credential.ID}
	u.server = u.serveUnisolated()
	waitOnline(t, controller, m, registered.instance.ID, registered.credential.ID)
	return u
}

// serveUnisolated starts the App Server with only an empty directory on its
// PATH.
func (u *userMachine) serveUnisolated() *cliProcess {
	u.t.Helper()
	return u.serveWith(map[string]string{"PATH": u.directory("empty-path")})
}

// requireRejected waits for the call's settlement and checks that the CLI
// rejected it with code.
func (s *runtimeSession) requireRejected(call toolCall, code string) settlement {
	t := s.r.e.t
	t.Helper()
	settled := s.await(call.use, time.Minute)
	if settled.Settlement != "rejected" || settled.Code != code {
		t.Fatalf("call %s settled %+v; want rejected %s", call.use, settled, code)
	}
	return settled
}

// errorsOfType is the Session's session.error events of type kind.
func errorsOfType(e *engine, sessionID, kind string) []sessionError {
	var found []sessionError
	for _, event := range e.developer().sessionErrors(e.t.Context(), sessionID) {
		if event.Error.Type == kind {
			found = append(found, event)
		}
	}
	return found
}

// logged reports whether log holds a record of event whose text includes
// each of fields.
func logged(log, event string, fields ...string) bool {
	for _, line := range strings.Split(log, "\n") {
		if strings.Contains(line, `"event":"`+event+`"`) && !slices.ContainsFunc(fields, func(field string) bool { return !strings.Contains(line, field) }) {
			return true
		}
	}
	return false
}

// withholdReports holds every session-status report that match accepts at
// the edge until its client gives up, so that Engine stores none of them,
// and sends each to the returned channel, while the rule is in place; the
// returned function removes the rule.
func withholdReports(e *engine, match func(protocol.SessionStatus) bool) (<-chan protocol.SessionStatus, func()) {
	withheld := make(chan protocol.SessionStatus, 16)
	remove := e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool {
			if r.Method != http.MethodPost || r.URL.Path != "/v1/client/session-status" {
				return false
			}
			body, err := io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewReader(body))
			report, decodeErr := protocol.DecodeSessionStatus(body)
			if err != nil || decodeErr != nil || !match(report) {
				return false
			}
			select {
			case withheld <- report:
			default:
			}
			return true
		},
		before: func(_ http.ResponseWriter, r *http.Request) bool {
			<-r.Context().Done()
			return true
		},
	})
	return withheld, remove
}

// readies keeps every ready the edge forwards, in arrival order. After
// holdNextConnection, the edge keeps the ACK of every ready of the next
// connection that sends one from its CLI, until the CLI gives up on it.
type readies struct {
	t    *testing.T
	mu   sync.Mutex
	seen []protocol.Ready
	// armed is set by holdNextConnection; held is the connection whose
	// readies' ACKs the edge keeps.
	armed bool
	held  string
}

// heldAck marks a ready whose ACK the edge keeps.
const heldAck = "X-Harness-Held-Ack"

// captureReadies puts the readies rule in place.
func captureReadies(e *engine) *readies {
	c := &readies{t: e.t}
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool { return r.Method == http.MethodPost && r.URL.Path == "/v1/client/ready" },
		before: func(_ http.ResponseWriter, r *http.Request) bool {
			body, err := io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewReader(body))
			ready, decodeErr := protocol.DecodeReady(body)
			if err != nil || decodeErr != nil {
				c.t.Errorf("a ready the edge forwarded does not decode: %v %v: %s", err, decodeErr, body)
				return false
			}
			c.mu.Lock()
			defer c.mu.Unlock()
			c.seen = append(c.seen, ready)
			if c.armed && c.held == "" {
				c.held = ready.ConnectionID
			}
			if c.held != "" && ready.ConnectionID == c.held {
				r.Header.Set(heldAck, "1")
			}
			return false
		},
		after: func(response *http.Response) error {
			if response.Request.Header.Get(heldAck) != "" {
				<-response.Request.Context().Done()
				return errLostAnswer
			}
			return nil
		},
	})
	return c
}

// holdNextConnection withholds the ACKs of the readies of the next
// connection that sends one.
func (c *readies) holdNextConnection() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.armed = true
}

// all is every ready forwarded so far.
func (c *readies) all() []protocol.Ready {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.seen)
}

// wait returns the first ready from index from on that match accepts, and
// its index, failing the test when none arrives within timeout.
func (c *readies) wait(from int, timeout time.Duration, match func(protocol.Ready) bool) (protocol.Ready, int) {
	c.t.Helper()
	for deadline := time.Now().Add(timeout); ; time.Sleep(50 * time.Millisecond) {
		seen := c.all()
		for i := from; i < len(seen); i++ {
			if match(seen[i]) {
				return seen[i], i
			}
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("no matching ready from the %d-th within %s", from, timeout)
		}
	}
}

// replay sends a request the edge captured to path through the edge again,
// with its headers and body, and returns the answer's status and body.
func replay(t *testing.T, e *engine, method, path string, captured capturedReport) (int, string) {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), method, e.serverURL+path, bytes.NewReader(captured.body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header = captured.headers.Clone()
	response, err := (&http.Client{Timeout: 30 * time.Second}).Do(request)
	if err != nil {
		t.Fatalf("replaying %s %s: %v", method, path, err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, string(body)
}

// controllerConnection is the connection the instance read names.
func controllerConnection(t *testing.T, e *engine, instanceID string) string {
	t.Helper()
	object := e.controller().readInstance(t.Context(), instanceID)
	if object.Connection == nil {
		t.Fatalf("instance %s reads no connection", instanceID)
	}
	return object.Connection.ID
}

// mountPoint is the Session's mount point of driveID on the machine.
func (m *machine) mountPoint(sessionID, driveID string) string {
	return m.home + "/sessions/" + sessionID + "/drives/" + driveID
}

// driveSyncReports keeps the bodies of the drive sync reports the edge
// passes on.
func driveSyncReports(e *engine) func() []driveSyncReport {
	var mu sync.Mutex
	var reports []driveSyncReport
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool {
			return r.Method == http.MethodPost && r.URL.Path == "/v1/client/drive-sync"
		},
		before: func(_ http.ResponseWriter, r *http.Request) bool {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				return false
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			var report driveSyncReport
			if json.Unmarshal(body, &report) == nil {
				mu.Lock()
				reports = append(reports, report)
				mu.Unlock()
			}
			return false
		},
	})
	return func() []driveSyncReport {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(reports)
	}
}

// driveSyncReport is the part of a drive sync report the scenarios read.
type driveSyncReport struct {
	SessionID string    `json:"session_id"`
	TaskID    string    `json:"task_id"`
	DriveSync driveSync `json:"drive_sync"`
}

// mountErrorsOf waits up to 30 seconds until the Session has want
// drive_mount_error events, then checks that it has exactly want, each for
// driveID on computerID.
func mountErrorsOf(t *testing.T, e *engine, sessionID, driveID, computerID string, want int) {
	t.Helper()
	errs := errorsOfType(e, sessionID, "drive_mount_error")
	for deadline := time.Now().Add(30 * time.Second); len(errs) < want && time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		errs = errorsOfType(e, sessionID, "drive_mount_error")
	}
	if len(errs) != want || slices.ContainsFunc(errs, func(e sessionError) bool { return e.Error.DriveID != driveID || e.Error.ComputerID != computerID }) {
		t.Fatalf("drive_mount_error events %+v; want %d, each for %s on %s", errs, want, driveID, computerID)
	}
}
