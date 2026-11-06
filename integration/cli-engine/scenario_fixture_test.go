package joint

import (
	"maps"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"
)

// scenario starts the Engine with options and the machines of a scenario.
func scenario(t *testing.T, options engineOptions) (*engine, *machines, *acceptanceRecord) {
	t.Helper()
	in := requireInputs(t, needs{})
	record := newAcceptanceRecord(t, in, platformOf(in))
	e := startEngine(t, record, options)
	return e, newMachines(t, in, needs{}, e, record), record
}

// userMachine is a CLI machine online on its own user instance.
type userMachine struct {
	*machine
	instanceID   string
	credentialID string
	server       *cliProcess
}

// onlineUserMachine registers a user instance, configures the machine name
// with it, starts its App Server and waits until it is online.
func onlineUserMachine(t *testing.T, e *engine, set *machines, name string) *userMachine {
	t.Helper()
	controller := e.controller()
	registered := controller.createUserInstance(t.Context(), name)
	m := set.add(name, name+"-app")
	m.configure(bootstrap(e.serverURL, registered.instance.ID, registered.credential))
	u := &userMachine{machine: m, instanceID: registered.instance.ID, credentialID: registered.credential.ID, server: m.serve()}
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
	ctx := t.Context()
	developer := e.developer()
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
	return session, computers, e.startRuntime().bind(session.ID)
}

// inDrive is the shell word of path in driveID's mount, for a command's
// text.
func inDrive(driveID, path string) string {
	return `"$TETRAL_DRIVES/` + driveID + "/" + path + `"`
}

// driveSync is the drive_sync of a command's result.
type driveSync struct {
	Committed int64 `json:"committed"`
	Deleted   int64 `json:"deleted"`
	Rejected  []struct {
		Path   string `json:"path"`
		Change string `json:"change"`
		Reason string `json:"reason"`
		Rule   string `json:"rule"`
	} `json:"rejected"`
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
