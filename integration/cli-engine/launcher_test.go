package joint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestLauncher runs the launcher's idle exit scenarios on one Engine.
func TestLauncher(t *testing.T) {
	scenarioTable(t, engineOptions{}, needs{},
		tableRow("idle_exit_upload", idleExitUploadsPendingChangesAfterTheIdleTime),
		tableRow("idle_exit_held_upload", idleExitLeavesAHeldUploadForTheNextStart),
	)
}

// writeOutside writes content to path in driveID's mount of sessionID on u
// from a process outside the CLI, which no tool call covers.
func writeOutside(t *testing.T, u *userMachine, sessionID, driveID, path, content string) {
	t.Helper()
	file := u.home + "/sessions/" + sessionID + "/drives/" + driveID + "/" + path
	if code, _ := u.shell("sh", "-c", "printf %s "+shellQuote(content)+" > "+shellQuote(file)); code != 0 {
		t.Fatalf("writing %s on %s exited %d", file, u.name, code)
	}
}

// waitIdleExit waits up to timeout until u's App Server exits and returns
// when it exited and its exit code.
func waitIdleExit(t *testing.T, u *userMachine, timeout time.Duration) (time.Time, int) {
	t.Helper()
	select {
	case <-u.server.exited:
	case <-time.After(timeout):
		t.Fatalf("server run on %s did not exit within %s", u.name, timeout)
	}
	exited := time.Now()
	var exit *exec.ExitError
	code := 0
	switch {
	case errors.As(u.server.err, &exit):
		code = exit.ExitCode()
	case u.server.err != nil:
		t.Fatalf("server run on %s: %v", u.name, u.server.err)
	}
	return exited, code
}

// idleExitUploadsPendingChangesAfterTheIdleTime starts server run with
// --idle-exit 1m on one machine and without it on another. After each
// machine's last command, a process outside the CLI writes a file into the
// Session's drive there. The first server exits no sooner than a minute
// after its last command was sent, once its file is committed; the second
// still runs after more than a minute and a half without a command.
func idleExitUploadsPendingChangesAfterTheIdleTime(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	idle := onlineUserMachine(t, e, set, "idle", "--idle-exit", "1m")
	kept := onlineUserMachine(t, e, set, "kept")
	session, computers, rs := driveSession(t, e, "idle", []sessionDrive{readWrite(driveID)}, idle, kept)
	keptLast := time.Now()
	rs.run(computers[1], "true", 2*time.Minute)
	writeOutside(t, kept, session.ID, driveID, "kept.txt", "kept")
	idleLast := time.Now()
	rs.run(computers[0], "true", 2*time.Minute)
	writeOutside(t, idle, session.ID, driveID, "idle.txt", "idle")

	exited, code := waitIdleExit(t, idle, 3*time.Minute)
	if code != 0 || exited.Sub(idleLast) < time.Minute {
		t.Fatalf("server run --idle-exit 1m exited %d after %s since its last command was sent; want 0, no sooner than a minute", code, exited.Sub(idleLast))
	}
	commits := e.edge.answered(http.MethodPost, commitRoute(driveID))
	changes := developer.listChanges(ctx, driveID, 0, "")
	if len(commits) != 1 || commits[0].status != http.StatusOK || commits[0].ended.After(exited) || len(changes) != 1 || changes[0].Path != "idle.txt" {
		t.Fatalf("commits %+v and change records %+v; want one commit of idle.txt answered before the exit at %s", commits, changes, exited)
	}
	if status, content := developer.readFile(ctx, driveID, "idle.txt"); status != http.StatusOK || content != "idle" {
		t.Fatalf("the drive serves idle.txt %d %q; want idle", status, content)
	}
	record.observe("server run --idle-exit 1m on %s exited 0 %s after its last command was sent; its commit of idle.txt was answered %s before the exit", idle.name, exited.Sub(idleLast).Round(time.Millisecond), exited.Sub(commits[0].ended).Round(time.Millisecond))

	time.Sleep(time.Until(keptLast.Add(90 * time.Second)))
	select {
	case <-kept.server.exited:
		t.Fatalf("server run without --idle-exit on %s exited", kept.name)
	default:
	}
	kept.waitStatus(30*time.Second, "online", func(status map[string]any) bool { return statusField(status, "connection_state") == "online" })
	record.observe("server run without --idle-exit on %s still ran and was online %s after its last command was sent", kept.name, time.Since(keptLast).Round(time.Second))
}

// idleExitLeavesAHeldUploadForTheNextStart holds the content upload of
// a file that a process outside the CLI wrote into a drive after the last
// command of a server run with --idle-exit 1m. The server exits within 30
// seconds after the idle minute plus the 5-second shutdown without
// committing the file, and no session.error is written; once the App Server
// starts again with uploads passing, the file is committed with no tool
// call.
func idleExitLeavesAHeldUploadForTheNextStart(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	m := onlineUserMachine(t, e, set, "idle", "--idle-exit", "1m")
	session, computers, rs := driveSession(t, e, "held", []sessionDrive{readWrite(driveID)}, m)
	held := regexp.MustCompile(`^/v1/client/drives/` + driveID + `/content/` + sha256Hex("held") + `$`)
	var holding atomic.Bool
	holding.Store(true)
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool {
			return r.Method == http.MethodPut && held.MatchString(r.URL.Path) && holding.Load()
		},
		before: func(_ http.ResponseWriter, r *http.Request) bool {
			<-r.Context().Done()
			return true
		},
	})
	rs.run(computers[0], "true", 2*time.Minute)
	answered := time.Now()
	writeOutside(t, m, session.ID, driveID, "held.txt", "held")

	exited, code := waitIdleExit(t, m, 3*time.Minute)
	// The idle minute starts no later than the command's result arrived.
	if code != 0 || exited.Sub(answered) > time.Minute+30*time.Second+5*time.Second {
		t.Fatalf("server run --idle-exit 1m exited %d %s after its last command's result; want 0 within a minute plus 35 seconds", code, exited.Sub(answered))
	}
	puts := e.edge.answered(http.MethodPut, held)
	if len(puts) == 0 || puts[0].at.After(exited) {
		t.Fatalf("held.txt's uploads %+v; want one sent before the exit", puts)
	}
	if commits := e.edge.answered(http.MethodPost, commitRoute(driveID)); len(commits) != 0 {
		t.Fatalf("commits %+v; want none before the next start", commits)
	}
	if errs := developer.sessionErrors(ctx, session.ID); len(errs) != 0 {
		t.Fatalf("session.error events %+v; want none", errs)
	}
	record.observe("server run --idle-exit 1m exited 0 %s after its last command's result, its upload of held.txt sent %s before the exit and held; nothing was committed and no session.error was written", exited.Sub(answered).Round(time.Millisecond), exited.Sub(puts[0].at).Round(time.Millisecond))

	holding.Store(false)
	m.waitOffline(e)
	m.server = m.serve()
	for deadline := time.Now().Add(2 * time.Minute); ; time.Sleep(time.Second) {
		if changes := developer.listChanges(ctx, driveID, 0, ""); len(changes) == 1 && changes[0].Path == "held.txt" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("held.txt was not committed within 2 minutes of the next start")
		}
	}
	if status, content := developer.readFile(ctx, driveID, "held.txt"); status != http.StatusOK || content != "held" {
		t.Fatalf("the drive serves held.txt %d %q; want held", status, content)
	}
	record.observe("after the next start of the App Server, held.txt was committed with no tool call")
}

// personAtTheMac is a scenario's channel to the person at the Mac: the
// directory ~/tetral-drives-consent, whose NEXT.txt says what to do now. The person
// runs each step's command in Terminal, so that Terminal launches both drives
// enable and the App Server and macOS names Terminal, and creates the file a
// step names once it is done.
type personAtTheMac struct {
	t      *testing.T
	host   macHost
	dir    string
	record *acceptanceRecord
	step   int
}

func newPersonAtTheMac(t *testing.T, m *machine, record *acceptanceRecord) *personAtTheMac {
	t.Helper()
	host, ok := m.host.(macHost)
	if !ok {
		t.Fatalf("%s is not a macOS machine", m.name)
	}
	dir := host.output(t, `printf %s "$HOME/tetral-drives-consent"`)
	host.output(t, "rm -rf "+shellQuote(dir)+" && mkdir "+shellQuote(dir))
	return &personAtTheMac{t: t, host: host, dir: dir, record: record}
}

// write writes content to name in the directory.
func (p *personAtTheMac) write(name, content string) {
	p.t.Helper()
	p.host.output(p.t, "printf %s "+shellQuote(content)+" > "+shellQuote(p.dir+"/"+name))
}

// read is name's trimmed content.
func (p *personAtTheMac) read(name string) string {
	p.t.Helper()
	return p.host.output(p.t, "cat "+shellQuote(p.dir+"/"+name))
}

// ask writes the next step to NEXT.txt and adds it to steps.txt.
func (p *personAtTheMac) ask(text string) {
	p.t.Helper()
	p.step++
	line := fmt.Sprintf("Step %d: %s\n", p.step, text)
	p.host.output(p.t, "printf %s "+shellQuote(line)+" | tee "+shellQuote(p.dir+"/NEXT.txt")+" >> "+shellQuote(p.dir+"/steps.txt"))
	p.record.command("asked the person at the Mac: %s", strings.TrimSpace(line))
}

// await waits until the directory holds one of names and returns it, failing
// the test after 20 minutes.
func (p *personAtTheMac) await(names ...string) string {
	p.t.Helper()
	deadline := time.Now().Add(20 * time.Minute)
	for {
		for _, name := range names {
			ctx, cancel := context.WithTimeout(p.t.Context(), 30*time.Second)
			err := p.host.remote(ctx, "test -e "+shellQuote(p.dir+"/"+name)).Run()
			cancel()
			if err == nil {
				return name
			}
		}
		if time.Now().After(deadline) {
			p.t.Fatalf("none of %v was created in %s within 20 minutes", names, p.dir)
		}
		time.Sleep(2 * time.Second)
	}
}

// enable writes enable-<n>.sh, which runs tetral drives enable on m with its
// process ID in enable-<n>.pid, its stdout in enable-<n>.out and its exit
// code in enable-<n>.exit, and returns the command that runs it.
func (p *personAtTheMac) enable(m *machine, n int) string {
	p.t.Helper()
	name := fmt.Sprintf("enable-%d", n)
	p.write(name+".sh", "#!/bin/sh\ncd "+shellQuote(p.dir)+" || exit 1\nTETRAL_HOME="+shellQuote(m.home)+" "+shellQuote(m.tetral)+
		" drives enable --app "+shellQuote(m.app)+" > "+name+".out 2> "+name+".err &\necho $! > "+name+".pid\nwait $!\necho $? > "+name+".exit\n")
	return "sh ~/tetral-drives-consent/" + name + ".sh"
}

// enabled waits for enable-<n>.exit and returns the exit code and stdout of
// that drives enable.
func (p *personAtTheMac) enabled(n int) (int, string) {
	p.t.Helper()
	name := fmt.Sprintf("enable-%d", n)
	p.await(name + ".exit")
	code, err := strconv.Atoi(p.read(name + ".exit"))
	if err != nil {
		p.t.Fatal(err)
	}
	out := p.read(name + ".out")
	p.record.command("`tetral drives enable` %d, run by the person in Terminal, exited %d: `%s`", n, code, out)
	return code, out
}

// serve writes serve-<machine>.sh, which runs tetral server run on m with
// its process ID in serve-<machine>.pid, and returns the command that runs
// it. An App Server still running when the test ends gets SIGTERM.
func (p *personAtTheMac) serve(m *machine) string {
	p.t.Helper()
	name := "serve-" + m.name
	p.write(name+".sh", "#!/bin/sh\necho $$ > "+shellQuote(p.dir+"/"+name+".pid")+"\nTETRAL_HOME="+shellQuote(m.home)+" exec "+shellQuote(m.tetral)+
		" server run --app "+shellQuote(m.app)+" > "+shellQuote(p.dir+"/"+name+".log")+" 2>&1\n")
	p.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		pid := shellQuote(p.dir + "/" + name + ".pid")
		_ = p.host.remote(ctx, "test -s "+pid+" && kill -TERM \"$(cat "+pid+")\" 2>/dev/null; true").Run()
	})
	return "sh ~/tetral-drives-consent/" + name + ".sh"
}

// stopServer ends the App Server that serve's script started and waits
// until it is gone.
func (p *personAtTheMac) stopServer(m *machine) {
	p.t.Helper()
	pid := p.read("serve-" + m.name + ".pid")
	p.host.output(p.t, "kill -TERM "+pid+" && while kill -0 "+pid+" 2>/dev/null; do sleep 0.2; done")
	p.host.output(p.t, "rm -f "+shellQuote(p.dir+"/serve-"+m.name+".pid"))
	p.record.command("SIGTERM to the App Server of %s that the person started", m.name)
}

// errorCode is the code of the {"error": {code, message}} object out.
func errorCode(out string) string {
	var object struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal([]byte(out), &object)
	return object.Error.Code
}

// TestDrivesEnableOnAMacRecordsOnlyConsent runs tetral drives enable on Macs
// with the person at the Mac, who runs each command in Terminal, answers
// macOS and says what macOS showed. Allowed, it records the consent: the
// next start mounts drives, and neither a command nor a restart makes macOS
// ask again or ask for a password. Denied, it records nothing and the Mac
// cannot mount; with the access turned on in System Settings it succeeds.
// Cancelled while macOS asks, it records nothing and leaves no mount. Run
// while the App Server runs, it fails and changes nothing.
func TestDrivesEnableOnAMacRecordsOnlyConsent(t *testing.T) {
	in := requireInputs(t, needs{platforms: []string{"macos"}, variables: []string{"TETRAL_PERSON_AT_MAC"}})
	record := newAcceptanceRecord(t, in, platformOf(in))
	e := startEngine(t, record, engineOptions{})
	set := newMachines(t, in, needs{}, e, record)
	ctx := t.Context()
	controller := e.controller()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	developer.upload(ctx, driveID, "f.txt", "drive")
	configured := func(name string) *userMachine {
		m := set.add(name, name+"-app")
		m.withoutDrives = true
		registered := controller.createUserInstance(ctx, name)
		m.configure(bootstrap(e.serverURL, registered.instance.ID, registered.credential))
		return &userMachine{machine: m, instanceID: registered.instance.ID, credentialID: registered.credential.ID}
	}
	mounts := func(u *userMachine) bool {
		t.Helper()
		instance := waitOnline(t, controller, u.machine, u.instanceID, u.credentialID)
		var capabilities struct {
			Drives struct {
				Mount bool `json:"mount"`
			} `json:"drives"`
		}
		if instance.Connection == nil || json.Unmarshal(instance.Connection.Capabilities, &capabilities) != nil {
			t.Fatalf("the instance's connection %+v; want ready", instance.Connection)
		}
		return capabilities.Drives.Mount
	}
	drivesEnabled := func(u *userMachine) bool {
		code, _ := u.shell("grep", "-qx", "drives_enabled = true", u.home+"/config.toml")
		return code == 0
	}
	const reset = "tccutil reset SystemPolicyNetworkVolumes com.apple.Terminal"

	allowed := configured("allowed")
	person := newPersonAtTheMac(t, allowed.machine, record)
	person.ask("In Terminal run: " + reset + " && " + person.enable(allowed.machine, 1) + " ; when macOS asks for access to network volumes, click Allow.")
	if code, out := person.enabled(1); code != 0 || out != "{}" || !drivesEnabled(allowed) {
		t.Fatalf("the allowed drives enable exited %d with %q; want 0, {} and drives_enabled recorded", code, out)
	}
	person.ask("In Terminal run: " + person.serve(allowed.machine))
	if !mounts(allowed) {
		t.Fatal("after the allowed drives enable the App Server's ready says it cannot mount")
	}
	session, computers, rs := driveSession(t, e, "allowed", []sessionDrive{readWrite(driveID)}, allowed)
	if out, _ := rs.run(computers[0], "cat "+inDrive(driveID, "f.txt")+" && printf mine > "+inDrive(driveID, "w.txt"), 2*time.Minute); out != "drive" {
		t.Fatalf("the command read %q; want drive", out)
	}
	previous := developer.readSessionComputer(ctx, session.ID, computers[0])
	person.stopServer(allowed.machine)
	person.ask("In Terminal run again: " + person.serve(allowed.machine))
	mounts(allowed)
	preparedAgain(e, session.ID, computers[0], *previous.BindingID)
	if out, _ := rs.run(computers[0], "cat "+inDrive(driveID, "w.txt"), 2*time.Minute); out != "mine" {
		t.Fatalf("after the restart the command read %q; want mine", out)
	}
	person.ask("Since you clicked Allow, has macOS asked anything again or asked for a password? If not, run: touch ~/tetral-drives-consent/none ; if it did, run: touch ~/tetral-drives-consent/asked")
	if person.await("none", "asked") == "asked" {
		t.Fatal("the person saw macOS ask again after the consent")
	}
	record.observe("allowed: drives enable exited 0 with {} and recorded drives_enabled; the next start mounted the drive, a command read and wrote it, a restart mounted it again, and the person saw macOS ask nothing more and no password dialog")
	person.stopServer(allowed.machine)

	denied := configured("denied")
	person.ask("In Terminal run: " + reset + " && " + person.enable(denied.machine, 2) + " ; when macOS asks, click Don't Allow.")
	if code, out := person.enabled(2); code != 1 || errorCode(out) != "drives_access_denied" || drivesEnabled(denied) {
		t.Fatalf("the denied drives enable exited %d with %q; want 1 drives_access_denied and nothing recorded", code, out)
	}
	denied.server = denied.serve()
	if mounts(denied) {
		t.Fatal("after the denied drives enable the App Server's ready says it can mount")
	}
	denied.server.stop()
	person.ask("In System Settings > Privacy & Security > Files and Folders, turn on Network Volumes for Terminal. Then in Terminal run: " + person.enable(denied.machine, 3))
	if code, out := person.enabled(3); code != 0 || out != "{}" || !drivesEnabled(denied) {
		t.Fatalf("drives enable after the access was turned on exited %d with %q; want 0 and {}", code, out)
	}
	record.observe("denied: drives enable exited 1 with drives_access_denied and recorded nothing, and the next start could not mount; with the access turned on in System Settings it exited 0 with {}")

	cancelled := configured("cancelled")
	person.ask("In Terminal run: " + reset + " && " + person.enable(cancelled.machine, 4) + " ; when macOS asks, do not answer it, and in another Terminal window run: touch ~/tetral-drives-consent/asking")
	person.await("asking")
	person.host.output(t, "kill -INT "+person.read("enable-4.pid"))
	record.command("SIGINT to drives enable 4 while macOS asked")
	if code, out := person.enabled(4); code != 1 || errorCode(out) != "cancelled" || drivesEnabled(cancelled) {
		t.Fatalf("the cancelled drives enable exited %d with %q; want 1 cancelled and nothing recorded", code, out)
	}
	if code, _ := cancelled.shell("sh", "-c", `mount | grep -qF "$1"`, "sh", cancelled.home+"/run/drives-enable"); code == 0 {
		t.Fatal("the cancelled drives enable left its test mount")
	}
	person.ask("If macOS still asks, click Allow. Then run: touch ~/tetral-drives-consent/answered")
	person.await("answered")
	record.observe("cancelled: SIGINT while macOS asked ended drives enable with cancelled; nothing was recorded and no test mount was left")

	allowed.server = allowed.serve()
	mounts(allowed)
	before := allowed.home + "/config.toml"
	_, config := allowed.shell("cat", before)
	if result := allowed.run(nil, "drives", "enable"); result.exitCode != 1 || errorCode(string(result.stdout)) != "server_running" {
		t.Fatalf("drives enable while the App Server runs exited %d with %s; want 1 server_running", result.exitCode, result.stdout)
	}
	if _, after := allowed.shell("cat", before); after != config {
		t.Fatalf("drives enable while the App Server runs changed config.toml from %q to %q", config, after)
	}
	record.observe("while the App Server ran, drives enable exited 1 with server_running and config.toml stayed as it was")
	person.ask("Done: nothing more to do.")
}
