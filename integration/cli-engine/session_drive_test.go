package joint

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestRepositoriesAreClonedOnlyOnTheirComputer gives one of a Session's two
// cloud computers two repositories, one with a mount_path and one without.
// Its machine clones the first to the mount_path and the second to
// <working_directory>/<repository name>, with origin the github.com URL; the
// other computer's machine clones nothing, and the global Git configuration
// of the machines' user is unchanged.
func TestRepositoriesAreClonedOnlyOnTheirComputer(t *testing.T) {
	in := requireInputs(t, needs{})
	record := newAcceptanceRecord(t, in, platformOf(in))
	record.boundary("the requests that answer 400, a user computer with repositories and two repositories with one path, create nothing and reach no machine; they are not repeated here")
	e := startEngine(t, record, engineOptions{})
	developer := e.developer()
	set := newMachines(t, in, needs{}, e, record)
	ctx := t.Context()

	appCommit := e.github.createRepository("acme", "app", "ghs_harness_app_token")
	docsCommit := e.github.createRepository("acme", "docs", "ghs_harness_docs_token")
	withRepositories, without := set.add("repositories", "provider-launcher"), set.add("plain", "provider-launcher")
	globalCode, globalConfig := withRepositories.shell("git", "config", "--global", "--list")
	mountPath := withRepositories.directory("docs-checkout")
	environments := []string{developer.createEnvironment(ctx, "repositories"), developer.createEnvironment(ctx, "plain")}
	session := developer.createSession(ctx, developer.createAgent(ctx, "repositories"),
		map[string]any{"environment_id": environments[0], "working_directory": withRepositories.directory("workspace"), "repositories": []any{
			map[string]any{"url": "https://github.com/acme/app", "authorization_token": "ghs_harness_app_token"},
			map[string]any{"url": "https://github.com/acme/docs", "authorization_token": "ghs_harness_docs_token", "mount_path": mountPath},
		}},
		map[string]any{"environment_id": environments[1], "working_directory": without.directory("workspace")})
	appPath := withRepositories.directory("workspace") + "/app"
	if paths := session.Computers[0].Repositories; len(paths) != 2 || paths[0].Path != appPath || paths[1].Path != mountPath {
		t.Fatalf("repositories of computer %s: %+v; want %s and %s", session.Computers[0].ID, paths, appPath, mountPath)
	}
	rs := e.startRuntime().bind(session.ID)
	startClaimedMachine(t, e, rs, withRepositories, cloudSession{sessionID: session.ID, computerID: session.Computers[0].ID, environmentID: environments[0]})
	startClaimedMachine(t, e, rs, without, cloudSession{sessionID: session.ID, computerID: session.Computers[1].ID, environmentID: environments[1]})

	for _, clone := range []struct{ path, url, commit string }{
		{appPath, "https://github.com/acme/app", appCommit},
		{mountPath, "https://github.com/acme/docs", docsCommit},
	} {
		head := gitOutput(t, withRepositories, "-C", clone.path, "rev-parse", "HEAD")
		origin := gitOutput(t, withRepositories, "-C", clone.path, "config", "--local", "--get", "remote.origin.url")
		if head != clone.commit || origin != clone.url {
			t.Fatalf("the clone at %s: HEAD %s, origin %s; want %s and %s", clone.path, head, origin, clone.commit, clone.url)
		}
	}
	if code, entries := without.shell("ls", "-A", without.directory("workspace")); code != 0 || entries != "" {
		t.Fatalf("the other computer's working directory: ls exited %d with %q; want it empty", code, entries)
	}
	if code, config := withRepositories.shell("git", "config", "--global", "--list"); code != globalCode || config != globalConfig {
		t.Fatal("the global Git configuration of the machines' user changed during the preparations")
	}
	record.observe("acme/app cloned to %s and acme/docs to its mount_path %s, each at its commit with origin its github.com URL; the other computer's working directory is empty; the global Git configuration of the machines' user unchanged", appPath, mountPath)
}

// TestReadOnlyBindingRefusesWritesAtTheOperatingSystem binds a drive
// read_only; a command's write into its mount fails with the operating
// system's read-only error and the drive gets no file.
func TestReadOnlyBindingRefusesWritesAtTheOperatingSystem(t *testing.T) {
	e, set, record := scenario(t, engineOptions{})
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	developer.upload(ctx, driveID, "kept.txt", "kept")
	m := onlineUserMachine(t, e, set, "reader")
	_, computers, rs := driveSession(t, e, "read-only", []sessionDrive{readOnly(driveID)}, m)

	out, envelope := rs.run(computers[0], "cat "+inDrive(driveID, "kept.txt")+"; echo; printf x > "+inDrive(driveID, "new.txt")+"; echo \"exit=$?\"", 2*time.Minute)
	if out != "kept\nexit=1" || !strings.Contains(envelope.Result.Stderr.Text, "new.txt: Read-only file system") {
		t.Fatalf("the command wrote %q and %q; want the read of kept.txt and the write refused as a read-only file system", out, envelope.Result.Stderr.Text)
	}
	if status, _ := developer.readFile(ctx, driveID, "new.txt"); status != http.StatusNotFound {
		t.Fatalf("reading new.txt from the drive answered %d; want 404", status)
	}
	record.observe("through the read_only binding the command read kept.txt and its write of new.txt failed: %q; the drive has no new.txt", envelope.Result.Stderr.Text)
}

// TestDriveAddedToAnIdleSessionMountsBeforeTheNextCommand creates a Session
// without drives, which binds none and creates none, then binds a drive while
// the Session is idle: the addition answers 201 with the binding, and the
// next command on each of its machines reads the drive.
func TestDriveAddedToAnIdleSessionMountsBeforeTheNextCommand(t *testing.T) {
	e, set, record := scenario(t, engineOptions{})
	ctx := t.Context()
	developer := e.developer()
	first, second := onlineUserMachine(t, e, set, "first"), onlineUserMachine(t, e, set, "second")
	session, computers, rs := driveSession(t, e, "added", nil, first, second)
	var drives struct {
		Data []json.RawMessage `json:"data"`
	}
	developer.get(ctx, "/v1/drives?limit=100", &drives)
	if len(session.Drives) != 0 || len(drives.Data) != 0 {
		t.Fatalf("the Session binds %+v and the Workspace has %d drives; want none", session.Drives, len(drives.Data))
	}
	record.observe("Session %s created without drives binds none, and the Workspace has no drive", session.ID)

	driveID := developer.createDrive(ctx)
	developer.upload(ctx, driveID, "seed.txt", "seed")
	status, body := developer.addDrive(ctx, session.ID, readWrite(driveID))
	var binding sessionDrive
	if status != http.StatusCreated || json.Unmarshal([]byte(body), &binding) != nil || binding != readWrite(driveID) {
		t.Fatalf("adding the drive answered %d %s; want 201 with the read_write binding", status, body)
	}
	for i, m := range []*userMachine{first, second} {
		if out, _ := rs.run(computers[i], "cat "+inDrive(driveID, "seed.txt"), 2*time.Minute); out != "seed" {
			t.Fatalf("the next command on %s read %q; want seed", m.name, out)
		}
	}
	record.observe("the addition answered 201 %s; the next command on each machine read seed.txt from the drive", body)
}

// TestVersionsNameTheirWriters writes one file through the API and one
// through a command: the API's version names the API key, the command's the
// Session and its computer.
func TestVersionsNameTheirWriters(t *testing.T) {
	e, set, record := scenario(t, engineOptions{})
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	developer.upload(ctx, driveID, "api.txt", "api")
	m := onlineUserMachine(t, e, set, "writer")
	session, computers, rs := driveSession(t, e, "writers", []sessionDrive{readWrite(driveID)}, m)
	rs.run(computers[0], "printf cli > "+inDrive(driveID, "cli.txt"), 2*time.Minute)

	api, cli := developer.listVersions(ctx, driveID, "api.txt"), developer.listVersions(ctx, driveID, "cli.txt")
	if len(api) != 1 || api[0].Writer.Type != "api_actor" || api[0].Writer.APIKeyID == "" || api[0].Writer.SessionID != "" {
		t.Fatalf("versions of api.txt %+v; want one written by an API key", api)
	}
	if len(cli) != 1 || cli[0].Writer != (driveWriter{Type: "session_actor", SessionID: session.ID, ComputerID: computers[0]}) {
		t.Fatalf("versions of cli.txt %+v; want one written by Session %s on computer %s", cli, session.ID, computers[0])
	}
	record.observe("api.txt's version names API key %s; cli.txt's names Session %s and computer %s", api[0].Writer.APIKeyID, session.ID, computers[0])
}

// TestEmptyFoldersStayLocal has one machine make an empty folder, which the
// other machine never lists, then delete the last file of a folder the other
// machine lists, which disappears from its next listing.
func TestEmptyFoldersStayLocal(t *testing.T) {
	e, set, record := scenario(t, engineOptions{})
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	developer.upload(ctx, driveID, "full/only.txt", "only")
	writer, lister := onlineUserMachine(t, e, set, "writer"), onlineUserMachine(t, e, set, "lister")
	_, computers, rs := driveSession(t, e, "folders", []sessionDrive{readWrite(driveID)}, writer, lister)
	list := func() []string {
		out, _ := rs.run(computers[1], "ls -A "+inDrive(driveID, ""), 2*time.Minute)
		return strings.Fields(out)
	}
	if listed := list(); !slices.Equal(listed, []string{"full"}) {
		t.Fatalf("the lister's first listing %v; want [full]", listed)
	}
	rs.run(computers[0], "mkdir "+inDrive(driveID, "empty"), 2*time.Minute)
	if listed := list(); !slices.Equal(listed, []string{"full"}) {
		t.Fatalf("the lister's listing after the empty folder %v; want [full]", listed)
	}
	rs.run(computers[0], "rm "+inDrive(driveID, "full/only.txt"), 2*time.Minute)
	if listed := list(); len(listed) != 0 {
		t.Fatalf("the lister's listing after the deletion %v; want nothing", listed)
	}
	record.observe("the lister never listed the writer's empty folder; after the writer deleted full/only.txt, full disappeared from the lister's next listing")
}

// TestBackgroundWritesUploadWithoutAToolCall runs a command in the
// background that writes a drive file: its content uploads 2 seconds after
// the last write and is committed with no other tool call.
func TestBackgroundWritesUploadWithoutAToolCall(t *testing.T) {
	e, set, record := scenario(t, engineOptions{})
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	m := onlineUserMachine(t, e, set, "writer")
	_, computers, rs := driveSession(t, e, "background", []sessionDrive{readWrite(driveID)}, m)
	written := m.directory("background") + "/written.at"
	call := rs.execInput(computers[0], map[string]any{"cmd": "sleep 3; printf late > " + inDrive(driveID, "late.txt") + "; date +%s.%N > '" + written + "'; sleep 60", "wait_ms": 1000})
	if settled := rs.await(call.use, time.Minute); settled.Settlement != "report" || settled.Phase != "running" {
		t.Fatalf("the background call settled %+v; want a running report", settled)
	}
	deadline := time.Now().Add(time.Minute)
	var commits []edgeRequest
	for len(commits) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no commit of the background write within a minute")
		}
		time.Sleep(200 * time.Millisecond)
		commits = e.edge.answered(http.MethodPost, commitRoute(driveID))
	}
	_, stamp := m.shell("cat", written)
	seconds, err := strconv.ParseFloat(stamp, 64)
	if err != nil {
		t.Fatalf("the write's time %q: %v", stamp, err)
	}
	writeAt := time.Unix(0, int64(seconds*float64(time.Second)))
	puts := e.edge.answered(http.MethodPut, contentRoute(driveID))
	if len(puts) != 1 {
		t.Fatalf("%d content PUTs; want 1", len(puts))
	}
	// The time is taken just after the write, and the edge stamps the PUT on
	// arrival: 100 ms covers the two clocks' reading.
	if after := puts[0].at.Sub(writeAt); after < 1900*time.Millisecond || after > 4*time.Second {
		t.Fatalf("the content PUT arrived %s after the last write; want 2 seconds after it", after)
	}
	if status, content := developer.readFile(ctx, driveID, "late.txt"); status != http.StatusOK || content != "late" {
		t.Fatalf("reading late.txt answered %d %q; want 200 late", status, content)
	}
	record.observe("the background command wrote late.txt at %s; its content PUT arrived %s later and its commit at %s, with no tool call; the drive serves it", writeAt.Format(time.RFC3339Nano), puts[0].at.Sub(writeAt).Round(time.Millisecond), commits[0].at.Format(time.RFC3339Nano))
	rs.cancel(call)
}

// TestOneCommandCommitsASkillChangeOnce has one command change two files of
// a Skill: one commit holds both.
func TestOneCommandCommitsASkillChangeOnce(t *testing.T) {
	e, set, record := scenario(t, engineOptions{})
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	developer.upload(ctx, driveID, "skills/demo/SKILL.md", "---\nname: demo\ndescription: A demo Skill.\n---\nRun run.sh.\n")
	developer.upload(ctx, driveID, "skills/demo/run.sh", "echo one\n")
	before := len(developer.listChanges(ctx, driveID, 0, ""))
	m := onlineUserMachine(t, e, set, "editor")
	_, computers, rs := driveSession(t, e, "skill", []sessionDrive{readWrite(driveID)}, m)
	rs.run(computers[0], fmt.Sprintf("printf -- '---\\nname: demo\\ndescription: A changed Skill.\\n---\\nRun run.sh.\\n' > %s; printf 'echo two\\n' > %s",
		inDrive(driveID, "skills/demo/SKILL.md"), inDrive(driveID, "skills/demo/run.sh")), 2*time.Minute)
	changes := developer.listChanges(ctx, driveID, 0, "")[before:]
	if len(changes) != 2 || changes[0].Seq != changes[1].Seq || len(e.edge.answered(http.MethodPost, commitRoute(driveID))) != 1 {
		t.Fatalf("the command's change records %+v; want both files in one commit", changes)
	}
	record.observe("the command's two changes of skills/demo are one commit, seq %d", changes[0].Seq)
}

// TestCommandsReadTheirDriveButNotTheCLICredentials runs one command that
// reads its CLI's credential file and then its own Session's drive, both in
// the CLI's state root: the first read is denied, the second succeeds.
func TestCommandsReadTheirDriveButNotTheCLICredentials(t *testing.T) {
	e, set, record := scenario(t, engineOptions{})
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	developer.upload(ctx, driveID, "f.txt", "drive")
	m := onlineUserMachine(t, e, set, "machine")
	_, computers, rs := driveSession(t, e, "credentials", []sessionDrive{readOnly(driveID)}, m)
	credentials := m.home + "/credentials/credentials.json"
	if code, _ := m.shell("test", "-s", credentials); code != 0 {
		t.Fatalf("%s holds no credential file at %s", m.name, credentials)
	}
	envelope := rs.terminal(rs.exec(computers[0], "cat '"+credentials+"' >/dev/null; echo \"exit=$?\"; cat "+inDrive(driveID, "f.txt")), 2*time.Minute)
	if envelope.Result.Stdout.Text != "exit=1\ndrive" {
		t.Fatalf("the command wrote %q to stdout and %q to stderr; want the credential read refused and the drive read", envelope.Result.Stdout.Text, envelope.Result.Stderr.Text)
	}
	record.observe("reading %s exited 1 with %q; the same command then read f.txt from the Session's drive", credentials, envelope.Result.Stderr.Text)
}

// mounted reports whether the machine lists a mount at the Session's mount
// point of driveID.
func (u *userMachine) mounted(sessionID, driveID string) bool {
	u.t.Helper()
	code, _ := u.shell("sh", "-c", `mount | grep -qF "/sessions/$1/drives/$2 "`, "sh", sessionID, driveID)
	return code == 0
}

// TestDriveUnmountedOutsideTheCLIIsMountedAgain unmounts a Session's drive
// from outside the CLI: the drive is mounted again before the Session's next
// command, which reads it.
func TestDriveUnmountedOutsideTheCLIIsMountedAgain(t *testing.T) {
	e, set, record := scenario(t, engineOptions{})
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	developer.upload(ctx, driveID, "f.txt", "v1")
	m := onlineUserMachine(t, e, set, "machine")
	session, computers, rs := driveSession(t, e, "unmounted", []sessionDrive{readOnly(driveID)}, m)
	if out, _ := rs.run(computers[0], "cat "+inDrive(driveID, "f.txt"), 2*time.Minute); out != "v1" || !m.mounted(session.ID, driveID) {
		t.Fatalf("the first command read %q; want v1 from a mounted drive", out)
	}
	point := m.home + "/sessions/" + session.ID + "/drives/" + driveID
	code, out := m.shell("sh", "-c", `fusermount3 -u "$1" 2>&1 || fusermount -u "$1" 2>&1 || umount "$1" 2>&1`, "sh", point)
	if code != 0 || m.mounted(session.ID, driveID) {
		t.Fatalf("unmounting %s exited %d %s; want it unmounted", point, code, out)
	}
	record.observe("unmounted %s from outside the CLI", point)
	if out, _ := rs.run(computers[0], "cat "+inDrive(driveID, "f.txt"), 2*time.Minute); out != "v1" || !m.mounted(session.ID, driveID) {
		t.Fatalf("the next command read %q; want v1 from a mounted drive", out)
	}
	record.observe("the Session's next command read f.txt and the drive is mounted again")
}

// TestDriveExcludeLeavesOutPathsOnItsMachineOnly binds two drives, each with
// datasets/, to a Session whose computer on machine a excludes datasets/ of
// the first. On a the first drive has no datasets/ and the second keeps its
// own; machine b has both. Files uploaded after the Session started appear,
// except on a under the excluded folder.
func TestDriveExcludeLeavesOutPathsOnItsMachineOnly(t *testing.T) {
	e, set, record := scenario(t, engineOptions{})
	ctx := t.Context()
	developer := e.developer()
	excluded, other := developer.createDrive(ctx), developer.createDrive(ctx)
	for _, driveID := range []string{excluded, other} {
		developer.upload(ctx, driveID, "datasets/a.csv", "a")
		developer.upload(ctx, driveID, "keep.txt", "keep")
	}
	a, b := onlineUserMachine(t, e, set, "a"), onlineUserMachine(t, e, set, "b")
	exclude := map[string]any{"drive_exclude": []map[string]any{{"drive_id": excluded, "paths": []string{"datasets/"}}}}
	_, computers, rs := driveSessionWith(t, e, "exclude", []sessionDrive{readOnly(excluded), readOnly(other)}, []map[string]any{exclude}, a, b)
	developer.upload(ctx, excluded, "later.txt", "later")
	developer.upload(ctx, excluded, "datasets/later.csv", "later")
	everything := []string{
		excluded, excluded + "/datasets", excluded + "/datasets/a.csv", excluded + "/datasets/later.csv", excluded + "/keep.txt", excluded + "/later.txt",
		other, other + "/datasets", other + "/datasets/a.csv", other + "/keep.txt",
	}
	want := [][]string{
		slices.DeleteFunc(slices.Clone(everything), func(path string) bool { return strings.HasPrefix(path, excluded+"/datasets") }),
		everything,
	}
	for i, m := range []*userMachine{a, b} {
		out, _ := rs.run(computers[i], `cd "$TETRAL_DRIVES" && find . -mindepth 1 | sed 's|^\./||'`, 2*time.Minute)
		listed := strings.Split(out, "\n")
		slices.Sort(listed)
		slices.Sort(want[i])
		if !slices.Equal(listed, want[i]) {
			t.Fatalf("%s lists %q; want %q", m.name, listed, want[i])
		}
		record.observe("%s lists %q", m.name, listed)
	}
}

// TestPathsEqualUnderCaseFoldingCannotCoexist writes paths equal under case
// folding to existing ones: the API answers 409 naming the rule; a command's
// new Report.md beside report.md is removed with the rule reported; a
// command's rename of report.md to Report.md succeeds.
func TestPathsEqualUnderCaseFoldingCannotCoexist(t *testing.T) {
	e, set, record := scenario(t, engineOptions{})
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	developer.upload(ctx, driveID, "report.md", "report")
	developer.upload(ctx, driveID, "Straße.md", "strasse")
	for _, path := range []string{"Report.md", "STRASSE.md"} {
		status, header, body := developer.uploadFile(ctx, driveID, path, []byte("api"), nil)
		if status != http.StatusConflict || !strings.Contains(string(body), "case folding") || header.Get("X-Should-Retry") != "false" {
			t.Fatalf("the upload of %s answered %d %s with x-should-retry %q; want 409 naming the case folding rule, not to be retried", path, status, body, header.Get("X-Should-Retry"))
		}
		record.observe("the API upload of %s answered 409 %s", path, body)
	}
	m := onlineUserMachine(t, e, set, "machine")
	_, computers, rs := driveSession(t, e, "folding", []sessionDrive{readWrite(driveID)}, m)
	envelope := rs.terminal(rs.exec(computers[0], "printf agent > "+inDrive(driveID, "Report.md")), 2*time.Minute)
	if sync := envelope.DriveSync; sync == nil || len(sync.Rejected) != 1 || !strings.HasSuffix(sync.Rejected[0].Path, "/drives/"+driveID+"/Report.md") || sync.Rejected[0].Reason != "path_rule" || !strings.Contains(sync.Rejected[0].Rule, "case folding") {
		t.Fatalf("drive_sync %+v; want Report.md rejected by the case folding path rule", envelope.DriveSync)
	}
	if out, _ := rs.run(computers[0], `cd "$TETRAL_DRIVES/`+driveID+`" && ls`, 2*time.Minute); !slices.Equal(strings.Fields(out), []string{"Straße.md", "report.md"}) && !slices.Equal(strings.Fields(out), []string{"report.md", "Straße.md"}) {
		t.Fatalf("the drive holds %q after the rejection; want report.md and Straße.md", out)
	}
	record.observe("the command's new Report.md was rejected %+v and removed", envelope.DriveSync.Rejected[0])
	if _, envelope := rs.run(computers[0], `cd "$TETRAL_DRIVES/`+driveID+`" && mv report.md Report.md`, 2*time.Minute); len(envelope.DriveSync.Rejected) != 0 {
		t.Fatalf("the rename's drive_sync %+v; want no rejection", envelope.DriveSync)
	}
	var paths []string
	for _, file := range developer.listFiles(ctx, driveID) {
		paths = append(paths, file.Path)
	}
	slices.Sort(paths)
	if !slices.Equal(paths, []string{"Report.md", "Straße.md"}) {
		t.Fatalf("the drive lists %q after the rename; want Report.md and Straße.md", paths)
	}
	record.observe("the command's rename of report.md to Report.md was committed; the drive lists %q", paths)
}

// TestSessionChangesAreListedAndStreamed has a Session create, change and
// delete a file, one command each, while the API writes another:
// drives.changes.list with the Session's ID lists exactly the Session's
// three records, and drives.changes.stream signals the seq of every commit.
func TestSessionChangesAreListedAndStreamed(t *testing.T) {
	e, set, record := scenario(t, engineOptions{})
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	developer.upload(ctx, driveID, "changed.txt", "v1")
	developer.upload(ctx, driveID, "deleted.txt", "gone")
	seqs := developer.openChangeStream(ctx, driveID)
	signalled := func(what string) {
		t.Helper()
		changes := developer.listChanges(ctx, driveID, 0, "")
		last := changes[len(changes)-1].Seq
		select {
		case seq := <-seqs:
			if seq != last {
				t.Fatalf("the stream signalled seq %d after %s; want %d", seq, what, last)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("the stream signalled nothing within 30 s after %s", what)
		}
		record.observe("the stream signalled seq %d after %s", last, what)
	}
	signalled("the stream opened")
	m := onlineUserMachine(t, e, set, "machine")
	session, computers, rs := driveSession(t, e, "changes", []sessionDrive{readWrite(driveID)}, m)
	for _, command := range []string{"printf new > " + inDrive(driveID, "created.txt"), "printf v2 > " + inDrive(driveID, "changed.txt"), "rm " + inDrive(driveID, "deleted.txt")} {
		rs.run(computers[0], command, 2*time.Minute)
		signalled(command)
		developer.upload(ctx, driveID, "api.txt", command)
		signalled("an API upload")
	}
	var got []string
	for _, change := range developer.listChanges(ctx, driveID, 0, session.ID) {
		got = append(got, change.Path+" "+change.Operation)
	}
	if want := []string{"created.txt created", "changed.txt modified", "deleted.txt deleted"}; !slices.Equal(got, want) {
		t.Fatalf("the Session's change records are %q; want %q", got, want)
	}
	record.observe("drives.changes.list with session_id %s lists %q", session.ID, got)
}

// TestExecutableBitFollowsTheCurrentVersion uploads run.sh through the API,
// makes it executable on machine a, replaces its content through the API,
// then deletes and uploads it again. After each step the current version
// and both machines show: not executable, executable, executable, not
// executable.
func TestExecutableBitFollowsTheCurrentVersion(t *testing.T) {
	e, set, record := scenario(t, engineOptions{})
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	a, b := onlineUserMachine(t, e, set, "a"), onlineUserMachine(t, e, set, "b")
	_, computers, rs := driveSession(t, e, "executable", []sessionDrive{readWrite(driveID)}, a, b)
	executable := func(step string, want bool) {
		t.Helper()
		files := developer.listFiles(ctx, driveID)
		if len(files) != 1 || files[0].Executable != want {
			t.Fatalf("%s the drive lists %+v; want run.sh with executable %t", step, files, want)
		}
		for i, m := range []*userMachine{a, b} {
			out, _ := rs.run(computers[i], "if test -x "+inDrive(driveID, "run.sh")+"; then echo true; else echo false; fi", 2*time.Minute)
			if out != strconv.FormatBool(want) {
				t.Fatalf("%s %s shows run.sh executable %s; want %t", step, m.name, out, want)
			}
		}
		record.observe("%s the current version and both machines show executable %t", step, want)
	}
	developer.upload(ctx, driveID, "run.sh", "echo one")
	executable("after the first upload", false)
	rs.run(computers[0], "chmod +x "+inDrive(driveID, "run.sh"), 2*time.Minute)
	executable("after chmod +x on a", true)
	developer.upload(ctx, driveID, "run.sh", "echo two")
	executable("after the API replaced its content", true)
	if status := developer.deleteFile(ctx, driveID, "run.sh", "echo two"); status != http.StatusOK && status != http.StatusNoContent {
		t.Fatalf("deleting run.sh answered %d", status)
	}
	developer.upload(ctx, driveID, "run.sh", "echo three")
	executable("after its deletion and a new upload", false)
}

// TestInvalidNamesFailOnALinuxMount has a command create files whose names
// are not valid UTF-8, hold a tab or hold U+200B, rename a file to a name
// with a tab, and create a file whose name is in NFD. The first four fail
// with EINVAL and nothing of them is uploaded; the NFD file is committed
// under its NFC path.
func TestInvalidNamesFailOnALinuxMount(t *testing.T) {
	in := requireInputs(t, needs{platforms: []string{"linux"}})
	record := newAcceptanceRecord(t, in, platformOf(in))
	record.boundary("Linux only: the scenario is about a Linux mount")
	e := startEngine(t, record, engineOptions{})
	set := newMachines(t, in, needs{}, e, record)
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	m := onlineUserMachine(t, e, set, "machine")
	_, computers, rs := driveSession(t, e, "names", []sessionDrive{readWrite(driveID)}, m)
	script := `cd "$TETRAL_DRIVES/` + driveID + `"
for name in "$(printf 'bad\377.txt')" "$(printf 'tab\tname.txt')" "$(printf 'zero\342\200\213width.txt')"; do touch "$name" 2>&1; echo "exit=$?"; done
printf ok > ok.txt
mv ok.txt "$(printf 'renamed\t.txt')" 2>&1; echo "exit=$?"
printf nfd > "$(printf 'cafe\314\201.txt')"; echo "exit=$?"`
	out, _ := rs.run(computers[0], script, 2*time.Minute)
	lines := strings.Split(out, "\n")
	if len(lines) != 9 || lines[8] != "exit=0" {
		t.Fatalf("the command wrote %q; want four failures and the NFD write", out)
	}
	for i := 0; i < 8; i += 2 {
		// GNU mv words EINVAL from rename(2) as a move into a subdirectory
		// of itself.
		einval := strings.HasSuffix(lines[i], "Invalid argument") || i == 6 && strings.Contains(lines[i], "to a subdirectory of itself")
		if !einval || lines[i+1] != "exit=1" {
			t.Fatalf("the command wrote %q; want EINVAL for each invalid name", out)
		}
	}
	var paths []string
	for _, file := range developer.listFiles(ctx, driveID) {
		paths = append(paths, file.Path)
	}
	slices.Sort(paths)
	if !slices.Equal(paths, []string{"café.txt", "ok.txt"}) {
		t.Fatalf("the drive lists %q; want only café.txt in NFC and ok.txt", paths)
	}
	record.observe("the command wrote %q; the drive lists %q", out, paths)
}

// TestNeverSyncedPathsKeepTheMachinesOwnWrite uploads node_modules/x.js
// through the API: machine a reads the first and then a second upload; once
// a command on a writes its own content there, a reads it even after a third
// upload, and no commit of the Session names the path.
func TestNeverSyncedPathsKeepTheMachinesOwnWrite(t *testing.T) {
	e, set, record := scenario(t, engineOptions{})
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	developer.upload(ctx, driveID, "node_modules/x.js", "v1")
	m := onlineUserMachine(t, e, set, "a")
	session, computers, rs := driveSession(t, e, "never-synced", []sessionDrive{readWrite(driveID)}, m)
	read := func(want string) {
		t.Helper()
		if out, _ := rs.run(computers[0], "cat "+inDrive(driveID, "node_modules/x.js"), 2*time.Minute); out != want {
			t.Fatalf("a read %q; want %s", out, want)
		}
	}
	read("v1")
	developer.upload(ctx, driveID, "node_modules/x.js", "v2")
	read("v2")
	_, envelope := rs.run(computers[0], "printf mine > "+inDrive(driveID, "node_modules/x.js"), 2*time.Minute)
	developer.upload(ctx, driveID, "node_modules/x.js", "v3")
	read("mine")
	for _, change := range developer.listChanges(ctx, driveID, 0, session.ID) {
		t.Fatalf("the Session committed %+v; want no commit", change)
	}
	record.observe("a read v1, then v2; after its own write (drive_sync %+v) it read mine after the third upload; the Session has no change record", envelope.DriveSync)
}
