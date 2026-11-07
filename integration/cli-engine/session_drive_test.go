package joint

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/protocol"
)

// TestDriveFiles runs the drive scenarios of path rules, writers, versions,
// restore and Skills on one Engine.
func TestDriveFiles(t *testing.T) {
	scenarioTable(t, engineOptions{}, needs{},
		tableRow("case_folding", pathsEqualUnderCaseFoldingCannotCoexist),
		tableRow("invalid_names", invalidNamesFailOnALinuxMount).only("linux"),
		tableRow("version_writers", versionsNameTheirWriters),
		tableRow("executable_bit", executableBitFollowsTheCurrentVersion),
		tableRow("executable_files", executableFilesRunOnEveryMachine),
		tableRow("never_synced_paths", neverSyncedPathsKeepTheMachinesOwnWrite),
		tableRow("empty_folders", emptyFoldersStayLocal),
		tableRow("session_changes", sessionChangesAreListedAndStreamed),
		tableRow("restore", restoreBringsBackFilesACommandDeleted),
		tableRow("skill_change_commit", oneCommandCommitsASkillChangeOnce),
		tableRow("invalid_skill_edits", invalidSkillEditsCommitButInvalidArchivesAreRejected),
		tableRow("republished_skill", republishedSkillsReachSessionsWhole),
	)
}

// TestDriveBindings runs the drive scenarios of bindings, exclusions,
// repositories, permissions and computer changes on one Engine.
func TestDriveBindings(t *testing.T) {
	scenarioTable(t, engineOptions{}, needs{},
		tableRow("read_only_binding", readOnlyBindingRefusesWritesAtTheOperatingSystem),
		tableRow("drive_added_while_idle", driveAddedToAnIdleSessionMountsBeforeTheNextCommand),
		tableRow("binding_changes_while_idle", driveBindingsChangeOnlyWhileIdle),
		tableRow("binding_changes_reach_every_computer", bindingChangesReachEveryComputerBeforeItsNextCommand),
		tableRow("repositories", repositoriesAreClonedOnlyOnTheirComputer),
		tableRow("drive_exclude", driveExcludeLeavesOutPathsOnItsMachineOnly),
		tableRow("excluded_paths", excludedPathsCannotBeWritten),
		tableRow("credentials_denied", commandsReadTheirDriveButNotTheCLICredentials),
		tableRow("git_repository_in_drive", gitRepositoriesInADriveStayLocal),
		tableRow("removed_git_marker", removingAGitMarkerUploadsTheEditsMadeUnderIt),
		tableRow("offline_removal", offlineRemovalDeletesSessionDataWithoutUploads),
		tableRow("replaced_cloud_machine", driveContentOutlivesAReplacedCloudMachine),
	)
}

// TestDriveMounts runs the scenarios of drives mounted on machines and of
// their mount failures on one Engine.
func TestDriveMounts(t *testing.T) {
	scenarioTable(t, engineOptions{}, needs{},
		tableRow("mac_writes_reach_linux", macWritesReachLinuxMachines).only("macos"),
		tableRow("no_mount_support", machinesThatCannotMountPrepareWithoutDrives).only("macos"),
		tableRow("drives_variable", drivesVariableNamesTheSessionsDrivesOnEveryPlatform).only("macos"),
		tableRow("leftover_mount_points", mountPointsLeftByAKilledAppServer),
		tableRow("stopped_mounting", driveThatStopsMountingIsReportedOnce),
		tableRow("unmounted_outside_cli", driveUnmountedOutsideTheCLIIsMountedAgain),
		tableRow("lasting_mount_failures", lastingMountFailuresAreReportedOncePerMachine),
		tableRow("shared_mac_content", oneMacStoresADrivesContentOnce).only("macos"),
		tableRow("mac_search", searchingAMacMountMatchesLocalDisk).only("macos"),
	)
}

// TestDriveSync runs the drive scenarios of reads, uploads, commits, deletes
// and remote changes on one Engine.
func TestDriveSync(t *testing.T) {
	scenarioTable(t, engineOptions{}, needs{},
		tableRow("waited_download", runningResultListsAWaitedDownload),
		tableRow("download_retry", downloadsWaitForStorageAndFailWhenRefused),
		tableRow("long_mac_download", longDownloadsAndShortPausesEndWithoutErrorOnAMac).only("macos"),
		tableRow("remote_changes_on_mac", remoteChangesReachAMacMount).only("macos"),
		tableRow("background_write", backgroundWritesUploadWithoutAToolCall),
		tableRow("upload_outlasting_wait", uploadsOutlastingTheWaitDoNotHoldCommands),
		tableRow("faulted_upload", faultedUploadsWaitForTheNextSyncPoint),
		tableRow("lost_commit_answer", lostCommitAnswersAreSentAgainBeforeTheRewrite),
		tableRow("removed_drive_held_upload", removedDriveReportsTheChangeItsHeldUploadLost),
		tableRow("concurrent_delete", deletingAFileChangedMeanwhileIsRejectedAndRestored),
		tableRow("content_verification", contentUploadsAreVerifiedPerDrive),
		tableRow("simultaneous_writes", simultaneousWritesOfOneFileLetOneWin),
		tableRow("large_upload", largeUploadsDoNotHoldCommands),
	)
}

// TestDriveConnectionLoss runs the drive scenarios in which a machine loses
// its connection, each row killing one computer-hub pod, on one Engine with
// four pods.
func TestDriveConnectionLoss(t *testing.T) {
	scenarioTable(t, engineOptions{hubPods: 4}, needs{},
		tableRow("session_data_stays", sessionDataStaysUntilTheArchiveUploadsIt),
		tableRow("pending_changes_outlive", pendingChangesOutliveConnectionsAndCrashes),
		tableRow("ended_bindings", endedBindingsFailWaitingReadsAndResumeTheDownloadOnDemand).only("linux"),
	)
}

// repositoriesAreClonedOnlyOnTheirComputer gives one of a Session's two
// cloud computers two repositories, one with a mount_path and one without.
// Its machine clones the first to the mount_path and the second to
// <working_directory>/<repository name>, with origin the github.com URL; the
// other computer's machine clones nothing, and the global Git configuration
// of the machines' user is unchanged.
func repositoriesAreClonedOnlyOnTheirComputer(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	record.boundary("the requests that answer 400, a user computer with repositories and two repositories with one path, create nothing and reach no machine; they are not repeated here")
	developer := e.developer()
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

// readOnlyBindingRefusesWritesAtTheOperatingSystem binds a drive
// read_only; a command's write into its mount fails with the operating
// system's read-only error and the drive gets no file.
func readOnlyBindingRefusesWritesAtTheOperatingSystem(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
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

// driveAddedToAnIdleSessionMountsBeforeTheNextCommand creates a Session
// without drives, which binds none and creates none, then binds a drive while
// the Session is idle: the addition answers 201 with the binding, and the
// next command on each of its machines reads the drive.
func driveAddedToAnIdleSessionMountsBeforeTheNextCommand(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	first, second := onlineUserMachine(t, e, set, "first"), onlineUserMachine(t, e, set, "second")
	drives := func() int {
		var listed struct {
			Data []json.RawMessage `json:"data"`
		}
		developer.get(ctx, "/v1/drives?limit=100", &listed)
		return len(listed.Data)
	}
	before := drives()
	session, computers, rs := driveSession(t, e, "added", nil, first, second)
	if after := drives(); len(session.Drives) != 0 || after != before {
		t.Fatalf("the Session binds %+v and the Workspace has %d drives, %d before the Session; want none bound and none created", session.Drives, after, before)
	}
	record.observe("Session %s created without drives binds none, and the Workspace has no new drive", session.ID)

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

// versionsNameTheirWriters writes one file through the API and one
// through a command: the API's version names the API key, the command's the
// Session and its computer.
func versionsNameTheirWriters(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
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

// emptyFoldersStayLocal has one machine make an empty folder, which the
// other machine never lists, then delete the last file of a folder the other
// machine lists, which disappears from its next listing.
func emptyFoldersStayLocal(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
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

// backgroundWritesUploadWithoutAToolCall runs a command in the
// background that writes a drive file: its content uploads 2 seconds after
// the last write. On Linux it is committed with no other tool call; on a
// Mac, whose mount does not learn the writing process, it is committed by
// the next command's sync point. The command runs for 2 minutes after the
// write, so that its own sync point commits nothing within either wait.
func backgroundWritesUploadWithoutAToolCall(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	m := onlineUserMachine(t, e, set, "writer")
	_, computers, rs := driveSession(t, e, "background", []sessionDrive{readWrite(driveID)}, m)
	written := m.directory("background") + "/written.at"
	call := rs.execInput(computers[0], map[string]any{"cmd": "sleep 3; printf late > " + inDrive(driveID, "late.txt") + "; date +%s.%N > '" + written + "'; sleep 120", "wait_ms": 1000})
	if settled := rs.await(call.use, time.Minute); settled.Settlement != "report" || settled.Phase != "running" {
		t.Fatalf("the background call settled %+v; want a running report", settled)
	}
	await := func(what string, requests func() []edgeRequest) []edgeRequest {
		t.Helper()
		for deadline := time.Now().Add(time.Minute); ; time.Sleep(200 * time.Millisecond) {
			if got := requests(); len(got) > 0 {
				return got
			}
			if time.Now().After(deadline) {
				t.Fatalf("no %s of the background write within a minute", what)
			}
		}
	}
	puts := await("content PUT", func() []edgeRequest { return e.edge.answered(http.MethodPut, contentRoute(driveID)) })
	commitBy := "with no tool call"
	if set.in.machineOS == "macos" {
		time.Sleep(5 * time.Second)
		if n := len(e.edge.answered(http.MethodPost, commitRoute(driveID))); n != 0 {
			t.Fatalf("%d commits within 5 seconds of the content PUT and before the next sync point; want none on a Mac", n)
		}
		if _, next := rs.run(computers[0], "true", 2*time.Minute); next.DriveSync == nil || next.DriveSync.Committed != 1 {
			t.Fatalf("the next command's drive_sync %+v; want late.txt committed", next.DriveSync)
		}
		commitBy = "by the next command's sync point, none in the 5 seconds after the PUT"
	}
	commits := await("commit", func() []edgeRequest { return e.edge.answered(http.MethodPost, commitRoute(driveID)) })
	_, stamp := m.shell("cat", written)
	seconds, err := strconv.ParseFloat(stamp, 64)
	if err != nil {
		t.Fatalf("the write's time %q: %v", stamp, err)
	}
	writeAt := time.Unix(0, int64(seconds*float64(time.Second)))
	if puts = e.edge.answered(http.MethodPut, contentRoute(driveID)); len(puts) != 1 {
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
	record.observe("the background command wrote late.txt at %s; its content PUT arrived %s later and its commit at %s, %s; the drive serves it", writeAt.Format(time.RFC3339Nano), puts[0].at.Sub(writeAt).Round(time.Millisecond), commits[0].at.Format(time.RFC3339Nano), commitBy)
	rs.cancel(call)
}

// oneCommandCommitsASkillChangeOnce has one command change two files of
// a Skill: one commit holds both.
func oneCommandCommitsASkillChangeOnce(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
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

// commandsReadTheirDriveButNotTheCLICredentials runs one command that
// reads its CLI's credential file and then its own Session's drive, both in
// the CLI's state root: the first read is denied, the second succeeds.
func commandsReadTheirDriveButNotTheCLICredentials(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
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
func (m *machine) mounted(sessionID, driveID string) bool {
	m.t.Helper()
	code, _ := m.shell("sh", "-c", `mount | grep -qF "/sessions/$1/drives/$2 "`, "sh", sessionID, driveID)
	return code == 0
}

// unmountOutside unmounts the Session's drive driveID from outside the CLI,
// lazily where the platform allows it, so that a mount whose server ended
// detaches too.
func (m *machine) unmountOutside(sessionID, driveID string) {
	m.t.Helper()
	point := m.mountPoint(sessionID, driveID)
	code, out := m.shell("sh", "-c", `fusermount3 -u -z "$1" 2>&1 || fusermount -u -z "$1" 2>&1 || umount -f "$1" 2>&1`, "sh", point)
	if code != 0 || m.mounted(sessionID, driveID) {
		m.t.Fatalf("unmounting %s exited %d %s; want it unmounted", point, code, out)
	}
	m.record.observe("unmounted %s from outside the CLI", point)
}

// driveUnmountedOutsideTheCLIIsMountedAgain unmounts a Session's drive
// from outside the CLI: the drive is mounted again before the Session's next
// command, which reads it.
func driveUnmountedOutsideTheCLIIsMountedAgain(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	developer.upload(ctx, driveID, "f.txt", "v1")
	m := onlineUserMachine(t, e, set, "machine")
	session, computers, rs := driveSession(t, e, "unmounted", []sessionDrive{readOnly(driveID)}, m)
	if out, _ := rs.run(computers[0], "cat "+inDrive(driveID, "f.txt"), 2*time.Minute); out != "v1" || !m.mounted(session.ID, driveID) {
		t.Fatalf("the first command read %q; want v1 from a mounted drive", out)
	}
	m.unmountOutside(session.ID, driveID)
	if out, _ := rs.run(computers[0], "cat "+inDrive(driveID, "f.txt"), 2*time.Minute); out != "v1" || !m.mounted(session.ID, driveID) {
		t.Fatalf("the next command read %q; want v1 from a mounted drive", out)
	}
	record.observe("the Session's next command read f.txt and the drive is mounted again")
}

// driveExcludeLeavesOutPathsOnItsMachineOnly binds two drives, each with
// datasets/, to a Session whose computer on machine a excludes datasets/ of
// the first. On a the first drive has no datasets/ and the second keeps its
// own; machine b has both. Files uploaded after the Session started appear,
// except on a under the excluded folder.
func driveExcludeLeavesOutPathsOnItsMachineOnly(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
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

// pathsEqualUnderCaseFoldingCannotCoexist writes paths equal under case
// folding to existing ones: the API answers 409 naming the rule; a command's
// new Report.md beside report.md is removed with the rule reported; a
// command's rename of report.md to Report.md succeeds.
func pathsEqualUnderCaseFoldingCannotCoexist(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
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

// sessionChangesAreListedAndStreamed has a Session create, change and
// delete a file, one command each, while the API writes another:
// drives.changes.list with the Session's ID lists exactly the Session's
// three records, and drives.changes.stream signals the seq of every commit.
func sessionChangesAreListedAndStreamed(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
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

// executableBitFollowsTheCurrentVersion uploads run.sh through the API,
// makes it executable on machine a, replaces its content through the API,
// then deletes and uploads it again. After each step the current version
// and both machines show: not executable, executable, executable, not
// executable.
func executableBitFollowsTheCurrentVersion(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	a, b := onlineUserMachine(t, e, set, "a"), onlineUserMachine(t, e, set, "b")
	_, computers, rs := driveSession(t, e, "executable", []sessionDrive{readWrite(driveID)}, a, b)
	shows := "if test -x " + inDrive(driveID, "run.sh") + "; then echo true; else echo false; fi"
	if set.in.machineOS == "macos" {
		// A Mac mount grants every access check, so test -x is true for any
		// file there; the mode shows the bit.
		shows = `case "$(stat -f %Sp ` + inDrive(driveID, "run.sh") + `)" in ???x*) echo true;; *) echo false;; esac`
		record.boundary("on a Mac the machines show the bit in the mode (stat); a file without it still runs there, as test -x is true for every file of the mount")
	}
	executable := func(step string, want bool) {
		t.Helper()
		files := developer.listFiles(ctx, driveID)
		if len(files) != 1 || files[0].Executable != want {
			t.Fatalf("%s the drive lists %+v; want run.sh with executable %t", step, files, want)
		}
		for i, m := range []*userMachine{a, b} {
			out, _ := rs.run(computers[i], shows, 2*time.Minute)
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

// invalidNamesFailOnALinuxMount has a command create files whose names
// are not valid UTF-8, hold a tab or hold U+200B, rename a file to a name
// with a tab, and create a file whose name is in NFD. The first four fail
// with EINVAL and nothing of them is uploaded; the NFD file is committed
// under its NFC path.
func invalidNamesFailOnALinuxMount(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	record.boundary("Linux only: the scenario is about a Linux mount")
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

// neverSyncedPathsKeepTheMachinesOwnWrite uploads node_modules/x.js
// through the API: machine a reads the first and then a second upload; once
// a command on a writes its own content there, a reads it even after a third
// upload, and no commit of the Session names the path.
func neverSyncedPathsKeepTheMachinesOwnWrite(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
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

// runningResultListsAWaitedDownload stalls the download of a file a
// command reads: the command's running result lists the file with the bytes
// downloaded so far and its size.
func runningResultListsAWaitedDownload(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	content := strings.Repeat("0123456789abcdef", 16384)
	developer.upload(ctx, driveID, "big.txt", content)
	route := regexp.MustCompile(`^/v1/client/drives/` + driveID + `/content/` + sha256Hex(content) + `$`)
	release := make(chan struct{})
	defer close(release)
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool { return r.Method == http.MethodGet && route.MatchString(r.URL.Path) },
		after: func(response *http.Response) error {
			response.Body = &heldBody{ReadCloser: response.Body, first: 4096, release: release}
			return nil
		},
	})
	m := onlineUserMachine(t, e, set, "reader")
	_, computers, rs := driveSession(t, e, "waiting", []sessionDrive{readOnly(driveID)}, m)
	call := rs.execInput(computers[0], map[string]any{"cmd": "sha256sum < " + inDrive(driveID, "big.txt"), "wait_ms": 2000})
	settled := rs.await(call.use, time.Minute)
	var envelope execEnvelope
	if settled.Settlement != "report" || settled.Phase != "running" || json.Unmarshal(settled.Result, &envelope) != nil || envelope.DriveSync == nil || len(envelope.DriveSync.Downloading) != 1 {
		t.Fatalf("the call settled %+v; want a running report listing one download", settled)
	}
	download := envelope.DriveSync.Downloading[0]
	if !strings.HasSuffix(download.Path, "/drives/"+driveID+"/big.txt") || download.DownloadedBytes != 4096 || download.SizeBytes != int64(len(content)) {
		t.Fatalf("the running result lists %+v; want big.txt with 4096 of %d bytes", download, len(content))
	}
	record.observe("the running result lists %s with %d of %d bytes downloaded", download.Path, download.DownloadedBytes, download.SizeBytes)
	rs.cancel(call)
}

// heldBody yields its first bytes, then nothing until release is closed or
// the request has ended.
type heldBody struct {
	io.ReadCloser
	first   int
	read    int
	release <-chan struct{}
}

func (b *heldBody) Read(p []byte) (int, error) {
	if b.read >= b.first {
		<-b.release
	} else if len(p) > b.first-b.read {
		p = p[:b.first-b.read]
	}
	n, err := b.ReadCloser.Read(p)
	b.read += n
	return n, err
}

// downloadsWaitForStorageAndFailWhenRefused answers a file's downloads
// 503 for ten seconds: the CLI tries again until the download succeeds, and
// the read returns the file. A download answered 403 drive_access_denied
// fails its read at once.
func downloadsWaitForStorageAndFailWhenRefused(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	developer.upload(ctx, driveID, "waits.txt", "waited")
	developer.upload(ctx, driveID, "refused.txt", "refused")
	waits := regexp.MustCompile(`^/v1/client/drives/` + driveID + `/content/` + sha256Hex("waited") + `$`)
	refused := regexp.MustCompile(`^/v1/client/drives/` + driveID + `/content/` + sha256Hex("refused") + `$`)
	var failingUntil atomic.Int64
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool { return r.Method == http.MethodGet && waits.MatchString(r.URL.Path) },
		before: func(w http.ResponseWriter, _ *http.Request) bool {
			failingUntil.CompareAndSwap(0, time.Now().Add(10*time.Second).UnixNano())
			if time.Now().UnixNano() >= failingUntil.Load() {
				return false
			}
			w.WriteHeader(http.StatusServiceUnavailable)
			return true
		},
	})
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool { return r.Method == http.MethodGet && refused.MatchString(r.URL.Path) },
		before: func(w http.ResponseWriter, _ *http.Request) bool {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write(protocol.Encode(protocol.ErrorBody{Code: protocol.ErrorDriveAccessDenied, Message: "the binding may not read this drive"}))
			return true
		},
	})
	m := onlineUserMachine(t, e, set, "reader")
	_, computers, rs := driveSession(t, e, "storage", []sessionDrive{readOnly(driveID)}, m)

	read := rs.execInput(computers[0], map[string]any{"cmd": "cat " + inDrive(driveID, "waits.txt"), "wait_ms": 50000})
	if envelope := rs.terminal(read, 2*time.Minute); envelope.Result.ExitCode == nil || *envelope.Result.ExitCode != 0 || envelope.Result.Stdout.Text != "waited" {
		t.Fatalf("the read of waits.txt ended %+v; want it to return waited", envelope)
	}
	attempts := e.edge.answered(http.MethodGet, waits)
	if len(attempts) < 2 || attempts[len(attempts)-1].status != http.StatusOK || attempts[len(attempts)-2].status != http.StatusServiceUnavailable {
		t.Fatalf("downloads of waits.txt %+v; want 503s, then 200", attempts)
	}
	record.observe("%d downloads of waits.txt were answered 503 over %s; the next was answered 200 and the read returned the file", len(attempts)-1, attempts[len(attempts)-1].at.Sub(attempts[0].at).Round(time.Millisecond))

	out, _ := rs.run(computers[0], "start=$(date +%s%N); cat "+inDrive(driveID, "refused.txt")+" 2>&1; echo \"exit=$? ms=$(( ($(date +%s%N) - start) / 1000000 ))\"", 2*time.Minute)
	var exit, took int
	if _, err := fmt.Sscanf(out[strings.LastIndex(out, "exit="):], "exit=%d ms=%d", &exit, &took); err != nil || exit == 0 || took > 5000 || !strings.Contains(out, "Input/output error") {
		t.Fatalf("the read of refused.txt wrote %q; want an I/O error within 5 seconds", out)
	}
	record.observe("the download of refused.txt answered 403 drive_access_denied and its read failed after %d ms: %q", took, out)
}

// driveBindingsChangeOnlyWhileIdle binds drive one to a Session: binding
// it again answers 409, removing it, the Session's last drive, succeeds, and
// binding it back succeeds. Once the Session runs, binding drive two answers
// 409, and a file uploaded into drive one reaches the Session before its
// next command.
func driveBindingsChangeOnlyWhileIdle(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	one, two := developer.createDrive(ctx), developer.createDrive(ctx)
	m := onlineUserMachine(t, e, set, "machine")
	session, computers, rs := driveSession(t, e, "bindings", []sessionDrive{readWrite(one)}, m)
	if status, body := developer.addDrive(ctx, session.ID, readWrite(one)); status != http.StatusConflict {
		t.Fatalf("binding drive one again answered %d %s; want 409", status, body)
	}
	if status := developer.removeDrive(ctx, session.ID, one); status != http.StatusNoContent {
		t.Fatalf("removing the Session's last drive answered %d; want 204", status)
	}
	if status, body := developer.addDrive(ctx, session.ID, readWrite(one)); status != http.StatusCreated {
		t.Fatalf("binding drive one back answered %d %s; want 201", status, body)
	}
	record.observe("binding a bound drive answered 409; removing the last drive answered 204; binding it back answered 201")
	rs.markRunning()
	if status, body := developer.addDrive(ctx, session.ID, readWrite(two)); status != http.StatusConflict {
		t.Fatalf("binding drive two to the running Session answered %d %s; want 409", status, body)
	}
	developer.upload(ctx, one, "while-running.txt", "uploaded")
	if out, _ := rs.run(computers[0], "cat "+inDrive(one, "while-running.txt"), 2*time.Minute); out != "uploaded" {
		t.Fatalf("the running Session's next command read %q; want the upload", out)
	}
	record.observe("while the Session ran, binding another drive answered 409, and its next command read a file uploaded into its drive")
}

// archiveFile is one file of a Skill archive.
type archiveFile struct {
	name, content string
	mode          os.FileMode
}

// skillMD is a SKILL.md of the Skill name with description.
func skillMD(name, description string) string {
	return "---\nname: " + name + "\ndescription: " + description + "\n---\nRun scripts/greet.sh.\n"
}

// zipSkill is a zip of files in the folder name.
func zipSkill(t *testing.T, name string, files ...archiveFile) []byte {
	t.Helper()
	var archive bytes.Buffer
	writer := zip.NewWriter(&archive)
	for _, file := range files {
		header := &zip.FileHeader{Name: name + "/" + file.name, Method: zip.Deflate}
		header.SetMode(file.mode)
		entry, err := writer.CreateHeader(header)
		if err == nil {
			_, err = entry.Write([]byte(file.content))
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return archive.Bytes()
}

// skillArchive is a zip of a Skill named name, in its folder name, with an
// executable script.
func skillArchive(t *testing.T, name string) []byte {
	t.Helper()
	return zipSkill(t, name, archiveFile{"SKILL.md", skillMD(name, "Prints a greeting."), 0o644}, archiveFile{"scripts/greet.sh", "#!/bin/sh\necho greeted\n", 0o755})
}

// executableFilesRunOnEveryMachine makes a script executable on machine
// a and runs it directly on machine b; a Skill zip with an executable script
// unpacks it executable, and b runs it directly too.
func executableFilesRunOnEveryMachine(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	a, b := onlineUserMachine(t, e, set, "a"), onlineUserMachine(t, e, set, "b")
	_, computers, rs := driveSession(t, e, "executable", []sessionDrive{readWrite(driveID)}, a, b)
	rs.run(computers[0], `cd "$TETRAL_DRIVES/`+driveID+`" && printf '#!/bin/sh\necho ran\n' > run.sh && chmod +x run.sh`, 2*time.Minute)
	if out, _ := rs.run(computers[1], `cd "$TETRAL_DRIVES/`+driveID+`" && ./run.sh`, 2*time.Minute); out != "ran" {
		t.Fatalf("./run.sh on b wrote %q; want ran", out)
	}
	record.observe("run.sh made executable on a ran directly on b")

	status, body := developer.uploadSkill(ctx, driveID, skillArchive(t, "greeter"))
	var skill struct {
		Name        string  `json:"name"`
		Description string  `json:"description"`
		Error       *string `json:"error"`
	}
	if status != http.StatusCreated || json.Unmarshal(body, &skill) != nil || skill.Name != "greeter" || skill.Error != nil {
		t.Fatalf("the Skill upload answered %d %s; want 201 with greeter and no error", status, body)
	}
	if files := developer.listFiles(ctx, driveID); !slices.ContainsFunc(files, func(file driveFile) bool {
		return file.Path == "skills/greeter/scripts/greet.sh" && file.Executable
	}) {
		t.Fatalf("the drive lists %+v; want skills/greeter/scripts/greet.sh executable", files)
	}
	if out, _ := rs.run(computers[1], `"$TETRAL_DRIVES/`+driveID+`/skills/greeter/scripts/greet.sh"`, 2*time.Minute); out != "greeted" {
		t.Fatalf("the Skill's script on b wrote %q; want greeted", out)
	}
	record.observe("the Skill upload answered 201 %s; its script unpacked executable and ran directly on b", body)
}

// gitRepositoriesInADriveStayLocal has a command create a Git
// repository in a drive: its result lists the repository once as
// git_repository, nothing of it is uploaded, and the next command's result
// does not list it again.
func gitRepositoriesInADriveStayLocal(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	m := onlineUserMachine(t, e, set, "machine")
	_, computers, rs := driveSession(t, e, "repository", []sessionDrive{readWrite(driveID)}, m)
	_, created := rs.run(computers[0], `cd "$TETRAL_DRIVES/`+driveID+`" && git init -q repo && printf x > repo/f.txt`, 2*time.Minute)
	if sync := created.DriveSync; sync == nil || len(sync.NotSynced) != 1 || !strings.HasSuffix(sync.NotSynced[0].Path, "/drives/"+driveID+"/repo") || sync.NotSynced[0].Reason != "git_repository" {
		t.Fatalf("the creating command's drive_sync %+v; want repo listed once as git_repository", created.DriveSync)
	}
	_, next := rs.run(computers[0], "printf y > "+inDrive(driveID, "other.txt"), 2*time.Minute)
	if sync := next.DriveSync; sync == nil || len(sync.NotSynced) != 0 || sync.Committed != 1 {
		t.Fatalf("the next command's drive_sync %+v; want other.txt committed and nothing listed", next.DriveSync)
	}
	var paths []string
	for _, file := range developer.listFiles(ctx, driveID) {
		paths = append(paths, file.Path)
	}
	if !slices.Equal(paths, []string{"other.txt"}) {
		t.Fatalf("the drive lists %q; want only other.txt", paths)
	}
	record.observe("the creating command listed %+v; the next command listed nothing; the drive lists %q", created.DriveSync.NotSynced, paths)
}

// deletingAFileChangedMeanwhileIsRejectedAndRestored has Session S1 on
// machine a delete f.txt just after Session S2 on machine b changed it,
// before a learned of the change: the deletion is rejected as a conflict and
// a has the current version again.
func deletingAFileChangedMeanwhileIsRejectedAndRestored(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	developer.upload(ctx, driveID, "f.txt", "v1")
	a, b := onlineUserMachine(t, e, set, "a"), onlineUserMachine(t, e, set, "b")
	s1, deleters, r1 := driveSession(t, e, "deleter", []sessionDrive{readWrite(driveID)}, a)
	_, changers, r2 := driveSession(t, e, "changer", []sessionDrive{readWrite(driveID)}, b)
	binding := *developer.waitComputer(ctx, s1.ID, deleters[0], time.Minute, "ready", func(c computerObject) bool { return c.ExecutionState == "ready" }).BindingID
	work := a.directory("deleter")
	call := r1.execInput(deleters[0], map[string]any{"cmd": "touch '" + work + "/started'; while [ ! -e '" + work + "/release' ]; do sleep 0.1; done; rm " + inDrive(driveID, "f.txt"), "wait_ms": 60000})
	for deadline := time.Now().Add(time.Minute); ; time.Sleep(200 * time.Millisecond) {
		if code, _ := a.shell("test", "-e", work+"/started"); code == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("S1's command did not start within a minute")
		}
	}
	// a learns of drive changes only through its change list requests, held
	// here until its deletion has been answered.
	learn := make(chan struct{})
	changes := regexp.MustCompile(`^/v1/client/drives/` + driveID + `/(changes|files)$`)
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool {
			return r.Method == http.MethodGet && changes.MatchString(r.URL.Path) && r.URL.Query().Get("binding_id") == binding
		},
		before: func(_ http.ResponseWriter, r *http.Request) bool {
			select {
			case <-learn:
			case <-r.Context().Done():
			}
			return false
		},
	})
	r2.run(changers[0], "printf v2 > "+inDrive(driveID, "f.txt"), 2*time.Minute)
	// A conflict is a rejected change inside an answered commit; the
	// deletion's commit is the first one after S2's.
	commits := len(e.edge.answered(http.MethodPost, commitRoute(driveID)))
	a.shell("touch", work+"/release")
	deadline := time.Now().Add(time.Minute)
	for len(e.edge.answered(http.MethodPost, commitRoute(driveID))) == commits {
		if time.Now().After(deadline) {
			t.Fatal("a sent no commit within a minute of the deletion")
		}
		time.Sleep(100 * time.Millisecond)
	}
	close(learn)
	envelope := r1.terminal(call, 2*time.Minute)
	if sync := envelope.DriveSync; sync == nil || len(sync.Rejected) != 1 || !strings.HasSuffix(sync.Rejected[0].Path, "/drives/"+driveID+"/f.txt") || sync.Rejected[0].Reason != "conflict" {
		t.Fatalf("the deletion's drive_sync %+v; want f.txt rejected as a conflict", envelope.DriveSync)
	}
	if out, _ := r1.run(deleters[0], "cat "+inDrive(driveID, "f.txt"), 2*time.Minute); out != "v2" {
		t.Fatalf("S1's next command read %q; want the current version v2", out)
	}
	if status, content := developer.readFile(ctx, driveID, "f.txt"); status != http.StatusOK || content != "v2" {
		t.Fatalf("the drive serves f.txt %d %q; want v2", status, content)
	}
	record.observe("S1's deletion was rejected %+v; S1's next command read v2, the drive's current version", envelope.DriveSync.Rejected[0])
}

// offlineRemovalDeletesSessionDataWithoutUploads removes a computer
// while its machine is offline with a change not uploaded; once the machine
// reconnects, the Session's data is deleted and nothing is uploaded.
func offlineRemovalDeletesSessionDataWithoutUploads(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	removeWhileOffline(t, e, set).reconnect(t, e)
}

// excludedPathsCannotBeWritten runs, on a computer whose drive_exclude
// leaves out datasets/, a command that lists the drive, makes the folder
// datasets, creates and appends to files in it and renames a file into it:
// datasets is never listed, making it fails with EACCES, the rest with
// ENOENT, and the drive records nothing; another computer reads
// datasets/a.csv unchanged.
func excludedPathsCannotBeWritten(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	developer.upload(ctx, driveID, "datasets/a.csv", "a")
	developer.upload(ctx, driveID, "notes.md", "notes")
	excluding, other := onlineUserMachine(t, e, set, "excluding"), onlineUserMachine(t, e, set, "other")
	exclude := map[string]any{"drive_exclude": []map[string]any{{"drive_id": driveID, "paths": []string{"datasets/"}}}}
	_, computers, rs := driveSessionWith(t, e, "exclude", []sessionDrive{readWrite(driveID)}, []map[string]any{exclude}, excluding, other)
	before := developer.listChanges(ctx, driveID, 0, "")
	script := `cd "$TETRAL_DRIVES/` + driveID + `" || exit 1
ls -A
mkdir datasets 2>&1; echo "exit=$?"
touch datasets/b.csv 2>&1; echo "exit=$?"
printf more | tee -a datasets/a.csv 2>&1 >/dev/null; echo "exit=$?"
mv notes.md datasets/notes.md 2>&1; echo "exit=$?"
ls -A`
	out, _ := rs.run(computers[0], script, 2*time.Minute)
	lines := strings.Split(out, "\n")
	want := []string{"notes.md", "Permission denied", "exit=1", "No such file or directory", "exit=1", "No such file or directory", "exit=1", "No such file or directory", "exit=1", "notes.md"}
	if len(lines) != len(want) {
		t.Fatalf("the command wrote %q; want %q at the ends of its lines", out, want)
	}
	for i := range want {
		if !strings.HasSuffix(lines[i], want[i]) {
			t.Fatalf("the command wrote %q; want %q at the ends of its lines", out, want)
		}
	}
	if after := developer.listChanges(ctx, driveID, 0, ""); len(after) != len(before) {
		t.Fatalf("the drive has %d change records after the command; want %d", len(after), len(before))
	}
	if out, _ := rs.run(computers[1], "cat "+inDrive(driveID, "datasets/a.csv"), 2*time.Minute); out != "a" {
		t.Fatalf("the other computer read %q; want datasets/a.csv unchanged", out)
	}
	record.observe("on the excluding computer the command wrote %q; the drive recorded no change and the other computer read datasets/a.csv unchanged", out)
}

// driveThatStopsMountingIsReportedOnce unmounts one of a Session's two
// drives from outside the CLI between two commands and puts a file at its
// mount point: the next command's result lists the drive in
// drive_sync.unmounted while the other drive still works, and one
// drive_mount_error is written; the command after it lists nothing and
// writes no second error.
func driveThatStopsMountingIsReportedOnce(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	stopping, other := developer.createDrive(ctx), developer.createDrive(ctx)
	developer.upload(ctx, other, "g.txt", "other")
	m := onlineUserMachine(t, e, set, "machine")
	session, computers, rs := driveSession(t, e, "stops", []sessionDrive{readOnly(stopping), readOnly(other)}, m)
	if _, first := rs.run(computers[0], "true", 2*time.Minute); first.DriveSync != nil && len(first.DriveSync.Unmounted) != 0 {
		t.Fatalf("the first command's drive_sync %+v; want no drive unmounted", first.DriveSync)
	}
	m.unmountOutside(session.ID, stopping)
	if code, out := m.shell("sh", "-c", `rmdir "$1" && touch "$1"`, "sh", m.mountPoint(session.ID, stopping)); code != 0 {
		t.Fatalf("replacing the mount point with a file exited %d %s", code, out)
	}
	out, next := rs.run(computers[0], "cat "+inDrive(other, "g.txt"), 2*time.Minute)
	if out != "other" || next.DriveSync == nil || len(next.DriveSync.Unmounted) != 1 || next.DriveSync.Unmounted[0].DriveID != stopping {
		t.Fatalf("the next command read %q with drive_sync %+v; want g.txt read and %s unmounted", out, next.DriveSync, stopping)
	}
	mountErrorsOf(t, e, session.ID, stopping, computers[0], 1)
	record.observe("the next command read the other drive and listed %s unmounted: %q; one drive_mount_error", stopping, next.DriveSync.Unmounted[0].Error)
	if _, after := rs.run(computers[0], "cat "+inDrive(other, "g.txt"), 2*time.Minute); after.DriveSync != nil && len(after.DriveSync.Unmounted) != 0 {
		t.Fatalf("the command after it had drive_sync %+v; want no drive listed again", after.DriveSync)
	}
	mountErrorsOf(t, e, session.ID, stopping, computers[0], 1)
	record.observe("the command after it listed no drive unmounted and no second drive_mount_error was written")
}

// mountPointsLeftByAKilledAppServer kills a machine's App Server, then,
// before it starts again, leaves one drive's stale mount as it is, unmounts a
// second drive and puts a file into its mount point, and unmounts a third
// and puts a file where its mount point was. The start prepares the Session
// within a minute: the first drive is mounted over its file, which is
// hidden, the stale mount is mounted again, and the third drive stays
// unmounted while the others and the commands work, with one
// drive_mount_error over two commands.
func mountPointsLeftByAKilledAppServer(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	covered, stale, blocked := developer.createDrive(ctx), developer.createDrive(ctx), developer.createDrive(ctx)
	for driveID, content := range map[string]string{covered: "covered", stale: "stale", blocked: "blocked"} {
		developer.upload(ctx, driveID, "f.txt", content)
	}
	m := onlineUserMachine(t, e, set, "machine")
	session, computers, rs := driveSession(t, e, "left", []sessionDrive{readOnly(covered), readOnly(stale), readOnly(blocked)}, m)
	first := *developer.readSessionComputer(ctx, session.ID, computers[0]).BindingID
	rs.run(computers[0], "cat "+inDrive(covered, "f.txt")+" "+inDrive(stale, "f.txt")+" "+inDrive(blocked, "f.txt"), 2*time.Minute)

	if err := m.server.signal(syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL to server run: %v", err)
	}
	<-m.server.exited
	if !m.mounted(session.ID, stale) {
		t.Fatalf("the killed App Server left no mount of %s", stale)
	}
	m.unmountOutside(session.ID, covered)
	if code, out := m.shell("sh", "-c", `printf earlier > "$1/earlier.txt"`, "sh", m.mountPoint(session.ID, covered)); code != 0 {
		t.Fatalf("writing into %s's mount point exited %d %s", covered, code, out)
	}
	m.unmountOutside(session.ID, blocked)
	if code, out := m.shell("sh", "-c", `rmdir "$1" && touch "$1"`, "sh", m.mountPoint(session.ID, blocked)); code != 0 {
		t.Fatalf("replacing %s's mount point with a file exited %d %s", blocked, code, out)
	}
	record.observe("killed the App Server; left %s's stale mount, put earlier.txt into %s's unmounted mount point and a file at %s's", stale, covered, blocked)

	m.waitOffline(e)
	started := time.Now()
	m.server = m.serve()
	developer.waitComputer(ctx, session.ID, computers[0], time.Minute, "prepared again", func(c computerObject) bool {
		return c.ExecutionState == "ready" && c.BindingID != nil && *c.BindingID != first
	})
	record.observe("the Session was prepared again %s after the start", time.Since(started).Round(time.Millisecond))
	mountErrorsOf(t, e, session.ID, blocked, computers[0], 1)
	for range 2 {
		out, envelope := rs.run(computers[0], "cat "+inDrive(covered, "f.txt")+"; echo; ls "+inDrive(covered, "")+"; cat "+inDrive(stale, "f.txt")+"; echo; cat "+inDrive(blocked, "f.txt")+" 2>/dev/null || echo unavailable", 2*time.Minute)
		if out != "covered\nf.txt\nstale\nunavailable" || envelope.DriveSync != nil && len(envelope.DriveSync.Unmounted) != 0 {
			t.Fatalf("the command wrote %q with drive_sync %+v; want the first two drives read, earlier.txt hidden, %s unavailable and nothing listed again", out, envelope.DriveSync, blocked)
		}
	}
	mountErrorsOf(t, e, session.ID, blocked, computers[0], 1)
	record.observe("two commands read the covered drive without earlier.txt and the remounted stale drive; %s stayed unavailable with one drive_mount_error", blocked)
}

// uploadsOutlastingTheWaitDoNotHoldCommands answers a drive's content
// uploads with 503 from before a command writes a file and exits until
// after the command after it: the first result arrives 30 seconds after the
// command exited with the file as uploading; the next command's result
// neither waits for the file nor lists it; once uploads pass, the file is
// committed with no tool call.
func uploadsOutlastingTheWaitDoNotHoldCommands(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	m := onlineUserMachine(t, e, set, "machine")
	_, computers, rs := driveSession(t, e, "storage", []sessionDrive{readWrite(driveID)}, m)
	var failing atomic.Bool
	failing.Store(true)
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool {
			return r.Method == http.MethodPut && contentRoute(driveID).MatchString(r.URL.Path) && failing.Load()
		},
		before: func(w http.ResponseWriter, _ *http.Request) bool {
			w.WriteHeader(http.StatusServiceUnavailable)
			return true
		},
	})
	sent := time.Now()
	_, first := rs.run(computers[0], "printf late > "+inDrive(driveID, "late.txt"), 2*time.Minute)
	took := time.Since(sent)
	if sync := first.DriveSync; sync == nil || sync.Committed != 0 || len(sync.NotSynced) != 1 || !strings.HasSuffix(sync.NotSynced[0].Path, "/drives/"+driveID+"/late.txt") || sync.NotSynced[0].Reason != "uploading" {
		t.Fatalf("the writing command's drive_sync %+v; want late.txt uploading", first.DriveSync)
	}
	if took < 30*time.Second || took > 40*time.Second {
		t.Fatalf("the writing command's result arrived %s after it was sent; want 30 seconds after its exit", took)
	}
	sent = time.Now()
	_, next := rs.run(computers[0], "true", 2*time.Minute)
	if took := time.Since(sent); took > 10*time.Second || next.DriveSync != nil && (len(next.DriveSync.NotSynced) != 0 || next.DriveSync.Committed != 0) {
		t.Fatalf("the next command's result arrived after %s with drive_sync %+v; want it at once, late.txt not listed", took, next.DriveSync)
	}
	refused := len(e.edge.answered(http.MethodPut, contentRoute(driveID)))
	record.observe("with uploads answered 503, the writing command's result arrived %s after it was sent with late.txt uploading; the next command's result arrived %s after it was sent without it; %d uploads were refused", took.Round(time.Millisecond), time.Since(sent).Round(time.Millisecond), refused)

	failing.Store(false)
	released := time.Now()
	for deadline := released.Add(2 * time.Minute); ; time.Sleep(time.Second) {
		if changes := developer.listChanges(ctx, driveID, 0, ""); len(changes) == 1 && changes[0].Path == "late.txt" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("late.txt was not committed within 2 minutes of uploads passing again")
		}
	}
	record.observe("late.txt was committed %s after uploads passed again, with no tool call", time.Since(released).Round(time.Second))
}

// faultedUploadsWaitForTheNextSyncPoint makes a drive change fault
// three ways: the hub refuses its upload with 400 invalid_message, the edge
// answers its commit with a 200 that fails the schema, and an upload held
// past the result's wait is refused that way once released, and again. Each
// next result lists the file as upload_pending, the change is tried again
// only at the next command's sync point, and it is committed once the
// faults stop; both logs hold the refusal's code, the CLI logs the malformed
// answer as a fault, and no session.error is written.
func faultedUploadsWaitForTheNextSyncPoint(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	m := onlineUserMachine(t, e, set, "machine")
	session, computers, rs := driveSession(t, e, "faults", []sessionDrive{readWrite(driveID)}, m)
	route := func(content string) *regexp.Regexp {
		return regexp.MustCompile(`^/v1/client/drives/` + driveID + `/content/` + sha256Hex(content) + `$`)
	}
	held, release := route("c"), make(chan struct{})
	var holding, refusing, malformed atomic.Bool
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool {
			return r.Method == http.MethodPut && held.MatchString(r.URL.Path) && holding.Load()
		},
		before: func(_ http.ResponseWriter, r *http.Request) bool {
			select {
			case <-release:
			case <-r.Context().Done():
			}
			r.Header.Set("Content-Type", "text/plain")
			return false
		},
	})
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool {
			return r.Method == http.MethodPut && contentRoute(driveID).MatchString(r.URL.Path) && refusing.Load()
		},
		before: func(_ http.ResponseWriter, r *http.Request) bool {
			r.Header.Set("Content-Type", "text/plain")
			return false
		},
	})
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool {
			return r.Method == http.MethodPost && commitRoute(driveID).MatchString(r.URL.Path) && malformed.Load()
		},
		before: func(w http.ResponseWriter, _ *http.Request) bool {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"results":"malformed"}`))
			return true
		},
	})
	pending := func(sync *driveSync, path string) bool { return sync.lists(driveID, path, "upload_pending") }
	// tried runs cmd and returns its result's drive_sync and how many uploads
	// of content were sent before it and during it.
	tried := func(cmd, content string) (*driveSync, int, int) {
		t.Helper()
		before := len(e.edge.answered(http.MethodPut, route(content)))
		_, envelope := rs.run(computers[0], cmd, 2*time.Minute)
		return envelope.DriveSync, before, len(e.edge.answered(http.MethodPut, route(content))) - before
	}
	committed := func(path string) {
		t.Helper()
		if sync, _, _ := tried("true", path[:1]); sync == nil || sync.Committed != 1 {
			t.Fatalf("the command after the faults stopped had drive_sync %+v; want %s committed", sync, path)
		}
	}

	refusing.Store(true)
	sync, _, _ := tried("printf a > "+inDrive(driveID, "a.txt"), "a")
	if !pending(sync, "a.txt") {
		t.Fatalf("the writing command's drive_sync %+v; want a.txt upload_pending", sync)
	}
	time.Sleep(3 * time.Second)
	sync, before, during := tried("true", "a")
	if !pending(sync, "a.txt") || during == 0 {
		t.Fatalf("the next command's drive_sync %+v after %d uploads during it; want a.txt upload_pending and tried again", sync, during)
	}
	record.observe("with uploads refused by the hub, a.txt was listed upload_pending; %d uploads were sent before the next command and %d during its sync point, which listed it again", before, during)
	refusing.Store(false)
	committed("a.txt")
	if !logged(e.hubs["computer-hub-0"].log.String(), "drive.transfer_refused", `"error.code":"invalid_message"`) ||
		!logged(m.server.stderr.String(), "drive.upload_fault", `"error.code":"invalid_message"`, `"http.response.status_code":400`) {
		t.Fatal("the computer-hub and CLI logs do not both hold the refusal's invalid_message")
	}

	malformed.Store(true)
	if sync, _, _ := tried("printf b > "+inDrive(driveID, "b.txt"), "b"); !pending(sync, "b.txt") {
		t.Fatalf("the writing command's drive_sync %+v with a malformed commit answer; want b.txt upload_pending", sync)
	}
	malformed.Store(false)
	if !logged(m.server.stderr.String(), "drive.upload_fault", `"error.code":"invalid_response"`, `"http.response.status_code":200`) {
		t.Fatal("the CLI log holds no fault for the commit answered by a 200 that fails the schema")
	}
	committed("b.txt")
	record.observe("with the commit answered by a 200 that fails the schema, b.txt was listed upload_pending, the CLI logged a fault invalid_response for the answer, and b.txt was committed at the next command")

	holding.Store(true)
	refusing.Store(true)
	if sync, _, _ := tried("printf c > "+inDrive(driveID, "c.txt"), "c"); !sync.lists(driveID, "c.txt", "uploading") {
		t.Fatalf("the writing command's drive_sync %+v with its upload held; want c.txt uploading", sync)
	}
	close(release)
	holding.Store(false)
	time.Sleep(3 * time.Second)
	sync, before, during = tried("true", "c")
	if !pending(sync, "c.txt") || during == 0 {
		t.Fatalf("the next command's drive_sync %+v after %d uploads during it; want c.txt upload_pending and tried again", sync, during)
	}
	refusing.Store(false)
	committed("c.txt")
	if errs := developer.sessionErrors(ctx, session.ID); len(errs) != 0 {
		t.Fatalf("session.error events %+v; want none", errs)
	}
	record.observe("c.txt, listed uploading while its upload was held and refused once released, was listed upload_pending by the next command, tried again there and committed after; no session.error was written")
}

// lostCommitAnswersAreSentAgainBeforeTheRewrite has Engine apply a sync
// point's commit of f while the edge loses its answer, and then answers the
// drive's commits with 503 for 60 seconds, while the next command rewrites
// f. Both results list f as uploading, the second without waiting for the
// first commit's answer; the CLI then sends
// the first commit again unchanged, which is accepted with no new version,
// and commits the rewrite after it with the first content as its base,
// which is accepted with a new version; no rejection reaches the agent.
func lostCommitAnswersAreSentAgainBeforeTheRewrite(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	m := onlineUserMachine(t, e, set, "machine")
	session, computers, rs := driveSession(t, e, "lost-commit", []sessionDrive{readWrite(driveID)}, m)
	reports := driveSyncReports(e)

	// sentCommit is one commit the CLI sent: its body, and the status and
	// body of its answer; status 0 is an answer the edge lost.
	type sentCommit struct {
		body   []byte
		status int
		answer []byte
	}
	const commitIndex = "X-Test-Commit-Index"
	var mu sync.Mutex
	var sent []*sentCommit
	var lostAt atomic.Pointer[time.Time]
	capture := func(r *http.Request) *sentCommit {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		commit := &sentCommit{body: body}
		mu.Lock()
		defer mu.Unlock()
		sent = append(sent, commit)
		r.Header.Set(commitIndex, strconv.Itoa(len(sent)-1))
		return commit
	}
	answered := func() []sentCommit {
		mu.Lock()
		defer mu.Unlock()
		var commits []sentCommit
		for _, commit := range sent {
			if commit.status == http.StatusOK {
				commits = append(commits, *commit)
			}
		}
		return commits
	}
	isCommit := func(r *http.Request) bool {
		return r.Method == http.MethodPost && commitRoute(driveID).MatchString(r.URL.Path)
	}
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool { return isCommit(r) && lostAt.Load() == nil },
		before: func(_ http.ResponseWriter, r *http.Request) bool {
			capture(r)
			now := time.Now()
			lostAt.Store(&now)
			return false
		},
		lose: true,
	})
	e.edge.addRule(&edgeRule{
		match: isCommit,
		before: func(w http.ResponseWriter, r *http.Request) bool {
			commit := capture(r)
			if time.Since(*lostAt.Load()) < time.Minute {
				mu.Lock()
				commit.status = http.StatusServiceUnavailable
				mu.Unlock()
				w.WriteHeader(http.StatusServiceUnavailable)
				return true
			}
			return false
		},
		after: func(response *http.Response) error {
			body, err := io.ReadAll(response.Body)
			if err != nil {
				return err
			}
			_ = response.Body.Close()
			response.Body = io.NopCloser(bytes.NewReader(body))
			index, err := strconv.Atoi(response.Request.Header.Get(commitIndex))
			if err != nil {
				return err
			}
			mu.Lock()
			sent[index].status, sent[index].answer = response.StatusCode, body
			mu.Unlock()
			return nil
		},
	})

	_, first := rs.run(computers[0], "printf one > "+inDrive(driveID, "f"), 2*time.Minute)
	lost := lostAt.Load()
	if lost == nil || !first.DriveSync.lists(driveID, "f", "uploading") || len(first.DriveSync.Rejected) != 0 {
		t.Fatalf("the first command's drive_sync %+v; want f uploading after its commit's answer was lost", first.DriveSync)
	}
	firstAt := time.Since(*lost)
	_, second := rs.run(computers[0], "printf two > "+inDrive(driveID, "f"), 2*time.Minute)
	secondAt := time.Since(*lost)
	early := len(answered())
	if !second.DriveSync.lists(driveID, "f", "uploading") || len(second.DriveSync.Rejected) != 0 || early != 0 {
		t.Fatalf("the rewriting command's drive_sync %+v with %d commits answered; want f uploading before any commit answer", second.DriveSync, early)
	}
	record.observe("the first commit's answer was lost; the first result arrived %s after that, the rewriting command's result %s after it, both listing f uploading, with every commit meanwhile answered 503", firstAt.Round(time.Millisecond), secondAt.Round(time.Millisecond))

	deadline := time.Now().Add(3 * time.Minute)
	for len(answered()) < 2 && time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
	}
	commits := answered()
	type commitAnswer struct {
		Seq     *int64 `json:"seq"`
		Results []struct {
			Status string `json:"status"`
		} `json:"results"`
	}
	accepted := func(commit sentCommit) (commitAnswer, bool) {
		var answer commitAnswer
		ok := json.Unmarshal(commit.answer, &answer) == nil && len(answer.Results) == 1 && answer.Results[0].Status == "accepted"
		return answer, ok
	}
	if len(commits) != 2 {
		t.Fatalf("%d commits were answered 200 within 3 minutes after the rewrite's result; want the first one again and the rewrite", len(commits))
	}
	mu.Lock()
	lostBody, refused := sent[0].body, len(sent)-1-len(commits)
	mu.Unlock()
	if replay, ok := accepted(commits[0]); !ok || replay.Seq != nil || !bytes.Equal(commits[0].body, lostBody) {
		t.Fatalf("the first commit answered 200 sent %s and was answered %s; want the lost commit %s again, accepted with no seq", commits[0].body, commits[0].answer, lostBody)
	}
	var rewrite struct {
		Changes []struct {
			Path    string  `json:"path"`
			Change  string  `json:"change"`
			Base    *string `json:"base_sha256"`
			Content string  `json:"content_sha256"`
		} `json:"changes"`
	}
	answer, ok := accepted(commits[1])
	if json.Unmarshal(commits[1].body, &rewrite) != nil || len(rewrite.Changes) != 1 || rewrite.Changes[0].Path != "f" || rewrite.Changes[0].Base == nil || *rewrite.Changes[0].Base != sha256Hex("one") || rewrite.Changes[0].Content != sha256Hex("two") || !ok || answer.Seq == nil {
		t.Fatalf("the next commit sent %s and was answered %s; want f rewritten on the first content's base, accepted with a seq", commits[1].body, commits[1].answer)
	}
	versions := developer.listVersions(ctx, driveID, "f")
	if len(versions) != 2 || versions[0].ContentSHA256 == nil || *versions[0].ContentSHA256 != sha256Hex("one") || versions[1].ContentSHA256 == nil || *versions[1].ContentSHA256 != sha256Hex("two") {
		t.Fatalf("f's versions %+v; want the first content and then the rewrite", versions)
	}
	for _, report := range reports() {
		if len(report.DriveSync.Rejected) != 0 {
			t.Fatalf("drive sync report %+v; want no rejection", report)
		}
	}
	if errs := developer.sessionErrors(ctx, session.ID); len(errs) != 0 {
		t.Fatalf("session.error events %+v; want none", errs)
	}
	record.observe("after %d commits answered 503, the lost commit was sent again unchanged and accepted with seq null, then the rewrite on base sha256(one) was accepted at seq %d; f has two versions and no rejection was reported", refused, *answer.Seq)
}

// removedDriveReportsTheChangeItsHeldUploadLost removes a read_write
// drive from an idle Session while the content upload of its one pending
// change is held for good. The session.drives report arrives within 30
// seconds of the instruction, plus the report round trip, with that path
// in lost_changes; one drive_changes_lost_error lists it with the binding's
// computer, paths_total 1 and a terminal retry_status; the drive keeps its
// old version.
func removedDriveReportsTheChangeItsHeldUploadLost(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	developer.upload(ctx, driveID, "f", "old")
	m := onlineUserMachine(t, e, set, "machine")
	session, computers, rs := driveSession(t, e, "removed-drive", []sessionDrive{readWrite(driveID)}, m)
	held := regexp.MustCompile(`^/v1/client/drives/` + driveID + `/content/` + sha256Hex("new") + `$`)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool { return r.Method == http.MethodPut && held.MatchString(r.URL.Path) },
		before: func(_ http.ResponseWriter, r *http.Request) bool {
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return false
		},
	})

	_, written := rs.run(computers[0], "printf new > "+inDrive(driveID, "f"), 2*time.Minute)
	if !written.DriveSync.lists(driveID, "f", "uploading") {
		t.Fatalf("the writing command's drive_sync %+v; want f uploading", written.DriveSync)
	}
	removed := time.Now()
	if status := developer.removeDrive(ctx, session.ID, driveID); status != http.StatusNoContent {
		t.Fatalf("removing the drive answered %d; want 204", status)
	}
	_, report := e.waitReport(2*time.Minute, "session.drives report", func(report protocol.SessionStatus) bool {
		return report.Status == "applied" && report.SessionID == session.ID
	})
	took := time.Since(removed)
	lost := report.LostChanges.Value
	if took > 35*time.Second || len(lost) != 1 || lost[0].DriveID != driveID || !slices.Equal(lost[0].Paths, []string{"f"}) || lost[0].PathsTotal != 1 || lost[0].RequestMessageID != report.RequestMessageID {
		t.Fatalf("the session.drives report %+v arrived %s after the removal; want f in lost_changes within 30 seconds plus the round trip", report, took)
	}
	errs := developer.lostChangeErrors(ctx, session.ID)
	if len(errs) != 1 || errs[0].Error.DriveID != driveID || errs[0].Error.ComputerID != computers[0] || !slices.Equal(errs[0].Error.Paths, []string{"f"}) || errs[0].Error.PathsTotal != 1 || errs[0].Error.RetryStatus.Type != "terminal" {
		t.Fatalf("drive_changes_lost_error events %+v; want one for f on %s with paths_total 1 and a terminal retry_status", errs, computers[0])
	}
	if status, content := developer.readFile(ctx, driveID, "f"); status != http.StatusOK || content != "old" {
		t.Fatalf("the drive's f reads %d %q; want the old version", status, content)
	}
	if versions := developer.listVersions(ctx, driveID, "f"); len(versions) != 1 {
		t.Fatalf("f's versions %+v; want only the old one", versions)
	}
	record.observe("with f's upload held, the session.drives report arrived %s after the removal with lost_changes %+v; one drive_changes_lost_error %q; the drive kept the old f", took.Round(time.Millisecond), lost, errs[0].Error.Message)
}

// removingAGitMarkerUploadsTheEditsMadeUnderIt makes a drive directory
// holding two drive files a Git repository, edits both files locally, and
// then removes .git without editing them again, while another writer
// changes the second file after the removal. Both edits upload without a new
// edit against the bases the machine recorded: the first is accepted and
// becomes the drive's content; the second is rejected as a conflict, as any
// change is, and the drive's version replaces it.
func removingAGitMarkerUploadsTheEditsMadeUnderIt(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	developer.upload(ctx, driveID, "repo/a.txt", "a1")
	developer.upload(ctx, driveID, "repo/b.txt", "b1")
	m := onlineUserMachine(t, e, set, "machine")
	_, computers, rs := driveSession(t, e, "git-marker", []sessionDrive{readWrite(driveID)}, m)
	held := regexp.MustCompile(`^/v1/client/drives/` + driveID + `/content/` + sha256Hex("b-local") + `$`)
	arrived, release := make(chan struct{}), make(chan struct{})
	var first sync.Once
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool { return r.Method == http.MethodPut && held.MatchString(r.URL.Path) },
		before: func(_ http.ResponseWriter, r *http.Request) bool {
			first.Do(func() { close(arrived) })
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return false
		},
	})
	repo := inDrive(driveID, "repo")

	if _, edited := rs.run(computers[0], "git init -q "+repo+" && printf a-local > "+repo+"/a.txt && printf b-local > "+repo+"/b.txt", 2*time.Minute); edited.DriveSync == nil || edited.DriveSync.Committed != 0 {
		t.Fatalf("the editing command's drive_sync %+v; want nothing committed under the Git repository", edited.DriveSync)
	}
	call := rs.exec(computers[0], "rm -rf "+repo+"/.git")
	select {
	case <-arrived:
	case <-time.After(time.Minute):
		t.Fatal("the edit of b.txt was not uploaded within a minute after .git was removed")
	}
	developer.upload(ctx, driveID, "repo/b.txt", "b-remote")
	close(release)
	removed := rs.finished(call, 2*time.Minute)
	outcome := removed.DriveSync
	if outcome == nil || outcome.Committed != 1 || len(outcome.Rejected) != 1 || !strings.HasSuffix(outcome.Rejected[0].Path, "repo/b.txt") || outcome.Rejected[0].Reason != "conflict" {
		t.Fatalf("the removing command's drive_sync %+v; want a.txt committed and b.txt rejected as a conflict", outcome)
	}
	if out, _ := rs.run(computers[0], "cat "+repo+"/a.txt "+repo+"/b.txt", 2*time.Minute); out != "a-localb-remote" {
		t.Fatalf("the machine reads %q; want its own a.txt and the drive's b.txt", out)
	}
	for path, want := range map[string]string{"repo/a.txt": "a-local", "repo/b.txt": "b-remote"} {
		if status, content := developer.readFile(ctx, driveID, path); status != http.StatusOK || content != want {
			t.Fatalf("the drive's %s reads %d %q; want %q", path, status, content, want)
		}
	}
	record.observe("removing .git uploaded both edits made under it with no new edit: a.txt was committed, b.txt, changed by the developer meanwhile, was rejected %+v and replaced by the drive's version", outcome.Rejected[0])
}

// restoreBringsBackFilesACommandDeleted commits 3,000 files, has a
// command delete them all and another create a file, and restores the drive
// to the moment before the deletion. The deleting result reports 3,000
// deletions; drives.restore answers restored: 3000; the next command reads
// every file back, and the later file, with no content uploaded again.
func restoreBringsBackFilesACommandDeleted(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	m := onlineUserMachine(t, e, set, "machine")
	session, computers, rs := driveSession(t, e, "restore", []sessionDrive{readWrite(driveID)}, m)
	// The files are written and uploaded outside any command, so that the
	// next sync point commits them at once.
	rs.runInBackground(computers[0])
	uploadOutside(e, m, session.ID, driveID, `mkdir dir && for i in $(seq -w 0 2999); do printf $i > dir/f$i; done`, 3000)
	if _, created := rs.run(computers[0], "true", 5*time.Minute); created.DriveSync == nil || created.DriveSync.Committed != 3000 {
		t.Fatalf("the command after the writes had drive_sync %+v; want 3,000 committed", created.DriveSync)
	}
	asOf := time.Now()
	// Deleting 3,000 files through the mount outlasts the default wait.
	deleted := rs.terminal(rs.execInput(computers[0], map[string]any{"cmd": "rm -rf " + inDrive(driveID, "dir"), "wait_ms": 50000}), 5*time.Minute)
	if deleted.Result.ExitCode == nil || *deleted.Result.ExitCode != 0 || deleted.DriveSync == nil || deleted.DriveSync.Deleted != 3000 {
		t.Fatalf("the deleting command ended %+v with drive_sync %+v; want exit 0 and 3,000 deletions", deleted.Result, deleted.DriveSync)
	}
	if _, later := rs.run(computers[0], "printf later > "+inDrive(driveID, "later.txt"), 2*time.Minute); later.DriveSync == nil || later.DriveSync.Committed != 1 {
		t.Fatalf("the later command's drive_sync %+v; want later.txt committed", later.DriveSync)
	}
	uploads := len(e.edge.answered(http.MethodPut, contentRoute(driveID)))
	status, answer := developer.doJSON(ctx, http.MethodPost, "/v1/drives/"+driveID+"/restore", map[string]any{"as_of": asOf.UTC().Format(time.RFC3339Nano)})
	var restored struct {
		Restored int `json:"restored"`
	}
	if status != http.StatusOK || json.Unmarshal(answer, &restored) != nil || restored.Restored != 3000 {
		t.Fatalf("drives.restore answered %d %s; want restored: 3000", status, answer)
	}
	if out, _ := rs.run(computers[0], "cd "+inDrive(driveID, "")+" && ls dir | wc -l && cat dir/f1234 later.txt", 5*time.Minute); out != "3000\n1234later" {
		t.Fatalf("the next command read %q; want 3,000 files back and later.txt", out)
	}
	if files := developer.listFiles(ctx, driveID); len(files) != 3001 {
		t.Fatalf("the drive lists %d files; want 3,001", len(files))
	}
	if again := len(e.edge.answered(http.MethodPut, contentRoute(driveID))) - uploads; again != 0 {
		t.Fatalf("%d uploads were sent after the restore; want none", again)
	}
	record.observe("a command's 3,000 deletions were reported and restored by drives.restore (restored: 3000) with later.txt kept; the next command read them back with no upload")
}

// contentUploadsAreVerifiedPerDrive changes one byte of the first upload
// of a file's content on its way to Engine, as a change to the file while
// its upload streams would, and then writes into one drive content that
// another drive of the Session already holds. Engine refuses the altered
// upload with content_hash_mismatch and records nothing; the CLI reads the
// file again and its next upload is accepted and committed. The content the
// other drive holds is uploaded again to the drive it is written into.
func contentUploadsAreVerifiedPerDrive(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID, otherID := developer.createDrive(ctx), developer.createDrive(ctx)
	m := onlineUserMachine(t, e, set, "machine")
	_, computers, rs := driveSession(t, e, "verified", []sessionDrive{readWrite(driveID), readWrite(otherID)}, m)
	// uploads is the status of every upload of content to the drive, in
	// arrival order.
	uploads := func(driveID, content string) []int {
		var statuses []int
		for _, r := range e.edge.answered(http.MethodPut, contentRoute(driveID)) {
			if strings.HasSuffix(r.path, "/"+sha256Hex(content)) {
				statuses = append(statuses, r.status)
			}
		}
		return statuses
	}
	accepted := func(status int) bool { return status == http.StatusOK || status == http.StatusCreated }
	committed := func(drive, path, content string) {
		t.Helper()
		if _, result := rs.run(computers[0], "printf '"+content+"' > "+inDrive(drive, path), 2*time.Minute); result.DriveSync == nil || result.DriveSync.Committed != 1 {
			t.Fatalf("the command writing %s had drive_sync %+v; want it committed", path, result.DriveSync)
		}
	}

	const content = "content of f"
	route := contentRoute(driveID)
	var altered atomic.Bool
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool {
			return r.Method == http.MethodPut && route.MatchString(r.URL.Path) && strings.HasSuffix(r.URL.Path, "/"+sha256Hex(content)) && altered.CompareAndSwap(false, true)
		},
		before: func(_ http.ResponseWriter, r *http.Request) bool {
			body, err := io.ReadAll(r.Body)
			if err != nil || len(body) == 0 {
				t.Errorf("reading the upload body to alter: %d bytes, %v", len(body), err)
			} else {
				body[0] ^= 1
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			return false
		},
	})
	committed(driveID, "f.txt", content)
	statuses := uploads(driveID, content)
	if len(statuses) != 2 || statuses[0] != http.StatusBadRequest || !accepted(statuses[1]) {
		t.Fatalf("the uploads of f.txt were answered %v; want 400 and then accepted", statuses)
	}
	if !slices.ContainsFunc(slices.Collect(maps.Values(e.hubs)), func(hub *serviceProcess) bool {
		return logged(hub.log.String(), "drive.transfer_refused", `"error.code":"content_hash_mismatch"`)
	}) {
		t.Fatal("no computer-hub log holds the refusal content_hash_mismatch")
	}
	changes := developer.listChanges(ctx, driveID, 0, "")
	files := developer.listFiles(ctx, driveID)
	if len(changes) != 1 || changes[0].Path != "f.txt" || len(files) != 1 || files[0].ContentSHA256 != sha256Hex(content) {
		t.Fatalf("the drive records %+v and lists %+v; want one version of f.txt with the written content", changes, files)
	}
	record.observe("the first upload of f.txt, with one byte changed on its way, was answered 400 content_hash_mismatch; the next upload was accepted and f.txt committed once with its content")

	const shared = "content of both drives"
	committed(otherID, "s.txt", shared)
	committed(driveID, "s.txt", shared)
	other, again := uploads(otherID, shared), uploads(driveID, shared)
	if len(other) == 0 || !accepted(other[len(other)-1]) || len(again) == 0 || !accepted(again[len(again)-1]) {
		t.Fatalf("the uploads of the shared content were answered %v to the other drive and %v to the drive; want each accepted", other, again)
	}
	if files := developer.listFiles(ctx, driveID); !slices.ContainsFunc(files, func(f driveFile) bool { return f.Path == "s.txt" && f.ContentSHA256 == sha256Hex(shared) }) {
		t.Fatalf("the drive lists %+v; want s.txt with the shared content", files)
	}
	record.observe("content the other drive held was uploaded again to the drive s.txt was written into (answers %v) and committed there", again)
}

// simultaneousWritesOfOneFileLetOneWin holds at the edge a command's
// commit of a new file and the developer's upload of the same path with
// must_not_exist, and releases both at once, six times. Each time exactly
// one of the two succeeds: the developer's upload is answered 201 and the
// agent's change is rejected as a conflict, or the change is committed and
// the upload is answered 409. The path has one version, and the machine then
// reads the winner's content.
func simultaneousWritesOfOneFileLetOneWin(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	m := onlineUserMachine(t, e, set, "machine")
	_, computers, rs := driveSession(t, e, "race", []sessionDrive{readWrite(driveID)}, m)
	commits, uploads := commitRoute(driveID), regexp.MustCompile(`^/v1/drives/`+driveID+`/files$`)
	// hold holds the next request of method to route until release closes,
	// and closes held when it arrives.
	hold := func(method string, route *regexp.Regexp, held, release chan struct{}) func() {
		var armed atomic.Bool
		armed.Store(true)
		return e.edge.addRule(&edgeRule{
			match: func(r *http.Request) bool {
				return r.Method == method && route.MatchString(r.URL.Path) && armed.CompareAndSwap(true, false)
			},
			before: func(http.ResponseWriter, *http.Request) bool {
				close(held)
				<-release
				return false
			},
		})
	}
	arrived := func(held chan struct{}, what string) {
		t.Helper()
		select {
		case <-held:
		case <-time.After(time.Minute):
			t.Fatalf("%s did not reach the edge within a minute", what)
		}
	}
	type answer struct {
		status int
		body   []byte
	}
	wins := map[string]int{}
	for i := range 6 {
		path := fmt.Sprintf("race%d.txt", i)
		heldCommit, heldUpload, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
		removeCommit, removeUpload := hold(http.MethodPost, commits, heldCommit, release), hold(http.MethodPost, uploads, heldUpload, release)
		call := rs.exec(computers[0], "printf agent > "+inDrive(driveID, path))
		arrived(heldCommit, "the commit of "+path)
		uploaded := make(chan answer, 1)
		go func() {
			status, _, body := developer.uploadFile(ctx, driveID, path, []byte("developer"), map[string]string{"must_not_exist": "true"})
			uploaded <- answer{status, body}
		}()
		arrived(heldUpload, "the upload of "+path)
		close(release)
		var upload answer
		select {
		case upload = <-uploaded:
		case <-time.After(time.Minute):
			t.Fatalf("the upload of %s was not answered within a minute", path)
		}
		removeCommit()
		removeUpload()
		result := rs.terminal(call, 2*time.Minute)
		if result.DriveSync == nil {
			t.Fatalf("the command writing %s reported no drive_sync", path)
		}
		sync := result.DriveSync
		conflict := len(sync.Rejected) == 1 && strings.HasSuffix(sync.Rejected[0].Path, "/"+path) && sync.Rejected[0].Reason == "conflict"
		winner := ""
		switch {
		case upload.status == http.StatusCreated && sync.Committed == 0 && conflict:
			winner = "developer"
		case upload.status == http.StatusConflict && sync.Committed == 1 && len(sync.Rejected) == 0:
			winner = "agent"
		default:
			t.Fatalf("for %s the upload was answered %d %s and the command's drive_sync was %+v; want exactly one of them to succeed", path, upload.status, upload.body, sync)
		}
		if versions := developer.listVersions(ctx, driveID, path); len(versions) != 1 {
			t.Fatalf("%s has versions %+v; want one", path, versions)
		}
		if out, _ := rs.run(computers[0], "cat "+inDrive(driveID, path), time.Minute); out != winner {
			t.Fatalf("the machine read %q from %s; want the %s's content", out, path, winner)
		}
		wins[winner]++
	}
	record.observe("in 6 rounds of a commit and an upload of the same path held at the edge and released at once, exactly one write won each time: developer %d, agent %d", wins["developer"], wins["agent"])
}

// pendingChangesOutliveConnectionsAndCrashes keeps a drive's uploads
// refused while a command writes a file, then ends the machine's connection
// by killing its computer-hub pod and holding its reconnects until the
// instance reads offline; does the same with a second file and a
// SIGKILL of the App Server, standing for a power loss; and kills the App
// Server once Engine has applied a third file's commit, before the answer
// reaches the CLI. After each reconnect or restart the next command's
// result arrives at once while uploads are still refused, without listing
// the file, and once uploads pass the file is committed with no tool call.
// The commit sent again after the crash carries the same changes and is
// accepted with no new version.
func pendingChangesOutliveConnectionsAndCrashes(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	m := onlineUserMachine(t, e, set, "machine")
	session, computers, rs := driveSession(t, e, "outlive", []sessionDrive{readWrite(driveID)}, m)
	var refusing atomic.Bool
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool {
			return r.Method == http.MethodPut && contentRoute(driveID).MatchString(r.URL.Path) && refusing.Load()
		},
		before: func(w http.ResponseWriter, _ *http.Request) bool {
			w.WriteHeader(http.StatusServiceUnavailable)
			return true
		},
	})
	binding := func() string { return *developer.readSessionComputer(ctx, session.ID, computers[0]).BindingID }
	restart := func() {
		t.Helper()
		<-m.server.exited
		m.waitOffline(e)
		m.server = m.serve()
	}
	// prompt runs a command, checks that its result arrives at once and
	// returns how long that took and its drive_sync.
	prompt := func() (time.Duration, *driveSync) {
		t.Helper()
		sent := time.Now()
		_, next := rs.run(computers[0], "true", 2*time.Minute)
		if took := time.Since(sent); took <= 10*time.Second {
			return took, next.DriveSync
		}
		t.Fatalf("the next command's result arrived %s after it was sent; want it at once", time.Since(sent))
		return 0, nil
	}
	committed := func(path string) time.Duration {
		t.Helper()
		released := time.Now()
		for deadline := released.Add(2 * time.Minute); ; time.Sleep(time.Second) {
			if versions := developer.listVersions(ctx, driveID, path); len(versions) == 1 {
				return time.Since(released)
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s was not committed within 2 minutes", path)
			}
		}
	}
	// outlive has a command write path while uploads are refused, then
	// loses the connection or the App Server with lose and checks the rest.
	outlive := func(path, loss string, lose func()) {
		t.Helper()
		refusing.Store(true)
		if _, written := rs.run(computers[0], "printf "+path+" > "+inDrive(driveID, path), 2*time.Minute); !written.DriveSync.lists(driveID, path, "uploading") {
			t.Fatalf("the command writing %s had drive_sync %+v; want it uploading", path, written.DriveSync)
		}
		previous := binding()
		lose()
		developer.waitComputer(ctx, session.ID, computers[0], 2*time.Minute, "prepared again", func(c computerObject) bool {
			return c.ExecutionState == "ready" && c.BindingID != nil && *c.BindingID != previous
		})
		took, sync := prompt()
		if sync != nil && (len(sync.NotSynced) != 0 || sync.Committed != 0) {
			t.Fatalf("the next command's drive_sync %+v; want %s neither committed nor listed", sync, path)
		}
		refusing.Store(false)
		record.observe("%s after %s was written and before its upload: with uploads still refused, the next command's result arrived %s after it was sent without it; once uploads passed it was committed %s later with no tool call", loss, path, took.Round(time.Millisecond), committed(path).Round(time.Second))
	}

	outlive("a.txt", "the connection ended (its computer-hub pod killed and the reconnects held until the instance read offline)", func() {
		hold := e.edge.holdEvents(m.instanceID)
		requests := e.edge.eventsRequests(m.instanceID)
		e.killHub(requests[len(requests)-1].backend)
		hold.waitArrived(t, time.Minute)
		m.waitOffline(e)
		e.edge.releaseEvents(m.instanceID)
	})
	outlive("b.txt", "the App Server was killed with SIGKILL", func() {
		if err := m.server.signal(syscall.SIGKILL); err != nil {
			t.Fatalf("SIGKILL to the App Server: %v", err)
		}
		restart()
	})

	// sentCommit is one commit after the crash: its body and answer.
	type sentCommit struct {
		body, answer []byte
		status       int
	}
	var mu sync.Mutex
	var crashedBody []byte
	var resent []sentCommit
	var crashed, armed atomic.Bool
	armed.Store(true)
	isCommit := func(r *http.Request) bool {
		return r.Method == http.MethodPost && commitRoute(driveID).MatchString(r.URL.Path)
	}
	readBody := func(r *http.Request) []byte {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		return body
	}
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool { return isCommit(r) && armed.CompareAndSwap(true, false) },
		before: func(_ http.ResponseWriter, r *http.Request) bool {
			body := readBody(r)
			mu.Lock()
			crashedBody = body
			mu.Unlock()
			return false
		},
		after: func(response *http.Response) error {
			if response.StatusCode == http.StatusOK {
				_ = m.server.signal(syscall.SIGKILL)
				crashed.Store(true)
			}
			return nil
		},
		lose: true,
	})
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool { return isCommit(r) && crashed.Load() },
		before: func(_ http.ResponseWriter, r *http.Request) bool {
			body := readBody(r)
			mu.Lock()
			resent = append(resent, sentCommit{body: body})
			r.Header.Set("X-Test-Commit-Index", strconv.Itoa(len(resent)-1))
			mu.Unlock()
			return false
		},
		after: func(response *http.Response) error {
			body, err := io.ReadAll(response.Body)
			if err != nil {
				return err
			}
			_ = response.Body.Close()
			response.Body = io.NopCloser(bytes.NewReader(body))
			index, err := strconv.Atoi(response.Request.Header.Get("X-Test-Commit-Index"))
			if err != nil {
				return err
			}
			mu.Lock()
			resent[index].status, resent[index].answer = response.StatusCode, body
			mu.Unlock()
			return nil
		},
	})
	previous := binding()
	rs.exec(computers[0], "printf c.txt > "+inDrive(driveID, "c.txt"))
	for deadline := time.Now().Add(2 * time.Minute); !crashed.Load(); time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("no commit of c.txt was applied within 2 minutes")
		}
	}
	restart()
	developer.waitComputer(ctx, session.ID, computers[0], 2*time.Minute, "prepared again", func(c computerObject) bool {
		return c.ExecutionState == "ready" && c.BindingID != nil && *c.BindingID != previous
	})
	// The commit sent again may still be in flight at the next command's
	// sync point, which then lists c.txt; it does not wait for it.
	took, sync := prompt()
	var first sentCommit
	for deadline := time.Now().Add(2 * time.Minute); ; time.Sleep(500 * time.Millisecond) {
		mu.Lock()
		if len(resent) > 0 && resent[0].status != 0 {
			first = resent[0]
		}
		mu.Unlock()
		if first.status != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the commit of c.txt was not sent again within 2 minutes of the restart")
		}
	}
	var before, again struct {
		Changes json.RawMessage `json:"changes"`
	}
	var answer struct {
		Seq     *int64 `json:"seq"`
		Results []struct {
			Status string `json:"status"`
		} `json:"results"`
	}
	mu.Lock()
	crashedCommit := crashedBody
	mu.Unlock()
	if json.Unmarshal(crashedCommit, &before) != nil || json.Unmarshal(first.body, &again) != nil || !bytes.Equal(before.Changes, again.Changes) ||
		first.status != http.StatusOK || json.Unmarshal(first.answer, &answer) != nil || answer.Seq != nil || len(answer.Results) != 1 || answer.Results[0].Status != "accepted" {
		t.Fatalf("after the crash the CLI sent %s, answered %d %s; want the changes of %s again, accepted with no seq", first.body, first.status, first.answer, crashedCommit)
	}
	if versions := developer.listVersions(ctx, driveID, "c.txt"); len(versions) != 1 {
		t.Fatalf("c.txt has versions %+v; want one", versions)
	}
	record.observe("the App Server was killed once Engine had answered c.txt's commit 200; after the restart the next command's result arrived %s after it was sent with drive_sync %+v, and the CLI sent the commit's changes again, accepted with seq null; c.txt has one version", took.Round(time.Millisecond), sync)
}

// invalidSkillEditsCommitButInvalidArchivesAreRejected has a command
// give a published Skill's SKILL.md a 2,000-character description, then
// publishes a zip carrying the same SKILL.md. The edit is committed like any
// file and drives.skills.list reports the Skill invalid with the reason; the
// zip is rejected with 400 invalid_request_error and changes nothing.
func invalidSkillEditsCommitButInvalidArchivesAreRejected(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	if status, body := developer.uploadSkill(ctx, driveID, skillArchive(t, "greeter")); status != http.StatusCreated {
		t.Fatalf("publishing the Skill answered %d %s; want 201", status, body)
	}
	m := onlineUserMachine(t, e, set, "machine")
	_, computers, rs := driveSession(t, e, "skill-check", []sessionDrive{readWrite(driveID)}, m)
	long := skillMD("greeter", strings.Repeat("a", 2000))
	if _, edited := rs.run(computers[0], "printf '%s' '"+long+"' > "+inDrive(driveID, "skills/greeter/SKILL.md"), 2*time.Minute); edited.DriveSync == nil || edited.DriveSync.Committed != 1 || len(edited.DriveSync.Rejected) != 0 {
		t.Fatalf("the editing command's drive_sync %+v; want SKILL.md committed", edited.DriveSync)
	}
	var listed struct {
		Data []struct {
			Name  string  `json:"name"`
			Error *string `json:"error"`
		} `json:"data"`
	}
	developer.get(ctx, "/v1/drives/"+driveID+"/skills", &listed)
	if len(listed.Data) != 1 || listed.Data[0].Name != "greeter" || listed.Data[0].Error == nil || !strings.Contains(*listed.Data[0].Error, "description") {
		t.Fatalf("drives.skills.list returned %+v; want greeter invalid for its description", listed.Data)
	}
	record.observe("the agent's edit of SKILL.md with a 2,000-character description was committed; drives.skills.list reports greeter with error %q", *listed.Data[0].Error)

	before := len(developer.listChanges(ctx, driveID, 0, ""))
	status, body := developer.uploadSkill(ctx, driveID, zipSkill(t, "greeter", archiveFile{"SKILL.md", long, 0o644}))
	var refused struct {
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	if status != http.StatusBadRequest || json.Unmarshal(body, &refused) != nil || refused.Error.Type != "invalid_request_error" {
		t.Fatalf("publishing a zip with the same SKILL.md answered %d %s; want 400 invalid_request_error", status, body)
	}
	if after := len(developer.listChanges(ctx, driveID, 0, "")); after != before {
		t.Fatalf("the rejected zip left %d change records; want none", after-before)
	}
	if files := developer.listFiles(ctx, driveID); !slices.ContainsFunc(files, func(f driveFile) bool {
		return f.Path == "skills/greeter/SKILL.md" && f.ContentSHA256 == sha256Hex(long)
	}) || !slices.ContainsFunc(files, func(f driveFile) bool { return f.Path == "skills/greeter/scripts/greet.sh" }) {
		t.Fatalf("the drive lists %+v; want the edited SKILL.md and the script kept", files)
	}
	record.observe("a zip carrying the same SKILL.md was answered 400 %s and changed nothing", body)
}

// pausedBody passes left bytes of a request body, then closes paused and
// waits for release before passing the rest.
type pausedBody struct {
	io.ReadCloser
	left            int64
	paused, release chan struct{}
}

func (b *pausedBody) Read(p []byte) (int, error) {
	if b.left == 0 {
		if b.paused != nil {
			close(b.paused)
			b.paused = nil
		}
		<-b.release
		return b.ReadCloser.Read(p)
	}
	if int64(len(p)) > b.left {
		p = p[:b.left]
	}
	n, err := b.ReadCloser.Read(p)
	b.left -= int64(n)
	return n, err
}

// republishedSkillsReachSessionsWhole publishes a Skill and has a
// running Session read it, then republishes it with a changed SKILL.md and
// script and without one of its files, holding the archive halfway on its
// way while a second Session starts and reads the Skill. The upload answers
// 201 with the Skill and no error; the Session started during the publish
// read the whole old Skill; the running Session's next command, and the
// started Session's, read the whole new one; the publish is one commit that
// deletes the file the new archive lacks.
func republishedSkillsReachSessionsWhole(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	version := func(n string) []byte {
		files := []archiveFile{{"SKILL.md", skillMD("greeter", "Greets, version "+n+"."), 0o644}, {"scripts/greet.sh", "#!/bin/sh\necho greeted " + n + "\n", 0o755}}
		if n == "1" {
			files = append(files, archiveFile{"old.txt", "only in version 1\n", 0o644})
		}
		return zipSkill(t, "greeter", files...)
	}
	whole := map[string]string{
		"1": "description: Greets, version 1.\ngreeted 1\nSKILL.md old.txt scripts",
		"2": "description: Greets, version 2.\ngreeted 2\nSKILL.md scripts",
	}
	read := `cd "$TETRAL_DRIVES/` + driveID + `/skills/greeter" && grep description SKILL.md && ./scripts/greet.sh && LC_ALL=C ls | tr '\n' ' '`
	reads := func(rs *runtimeSession, computerID, want, who string) {
		t.Helper()
		if out, _ := rs.run(computerID, read, 2*time.Minute); out != whole[want] {
			t.Fatalf("%s read the Skill as %q; want version %s whole", who, out, want)
		}
	}
	if status, body := developer.uploadSkill(ctx, driveID, version("1")); status != http.StatusCreated {
		t.Fatalf("publishing version 1 answered %d %s; want 201", status, body)
	}
	m := onlineUserMachine(t, e, set, "machine")
	drives := []sessionDrive{readWrite(driveID)}
	_, running, runningRS := driveSession(t, e, "running", drives, m)
	reads(runningRS, running[0], "1", "the running Session")
	before := developer.listChanges(ctx, driveID, 0, "")

	halfway, release := make(chan struct{}), make(chan struct{})
	skills := regexp.MustCompile(`^/v1/drives/` + driveID + `/skills$`)
	var armed atomic.Bool
	armed.Store(true)
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool {
			return r.Method == http.MethodPost && skills.MatchString(r.URL.Path) && armed.CompareAndSwap(true, false)
		},
		before: func(_ http.ResponseWriter, r *http.Request) bool {
			r.Body = &pausedBody{ReadCloser: r.Body, left: r.ContentLength / 2, paused: halfway, release: release}
			return false
		},
	})
	type answer struct {
		status int
		body   []byte
	}
	published := make(chan answer, 1)
	go func() {
		status, body := developer.uploadSkill(ctx, driveID, version("2"))
		published <- answer{status, body}
	}()
	select {
	case <-halfway:
	case <-time.After(time.Minute):
		t.Fatal("the publish of version 2 did not reach the edge within a minute")
	}
	_, starting, startingRS := driveSession(t, e, "starting", drives, m)
	reads(startingRS, starting[0], "1", "the Session started during the publish")
	close(release)
	var publish answer
	select {
	case publish = <-published:
	case <-time.After(time.Minute):
		t.Fatal("the publish of version 2 was not answered within a minute of its release")
	}
	var skill struct {
		Name        string  `json:"name"`
		Description string  `json:"description"`
		Error       *string `json:"error"`
	}
	if publish.status != http.StatusCreated || json.Unmarshal(publish.body, &skill) != nil || skill.Name != "greeter" || skill.Description != "Greets, version 2." || skill.Error != nil {
		t.Fatalf("publishing version 2 answered %d %s; want 201 with greeter, its description and no error", publish.status, publish.body)
	}
	reads(runningRS, running[0], "2", "the running Session's next command")
	reads(startingRS, starting[0], "2", "the started Session's next command")
	changes := developer.listChanges(ctx, driveID, 0, "")[len(before):]
	deleted := slices.ContainsFunc(changes, func(c driveChange) bool { return c.Path == "skills/greeter/old.txt" && c.Operation == "deleted" })
	if len(changes) != 3 || changes[0].Seq != changes[2].Seq || !deleted {
		t.Fatalf("the publish's change records %+v; want SKILL.md, the script and the deletion of old.txt in one commit", changes)
	}
	record.observe("version 2's archive was held halfway while a Session started and read version 1 whole; released, the publish answered 201 %s as one commit at seq %d that deleted old.txt, and both Sessions' next commands read version 2 whole", publish.body, changes[0].Seq)
}

// replaceCloudMachine has the Controller deregister instanceID, whose
// machine is gone, and serves the computer's next call, cmd, on next from a
// new request's claim; it returns the call once next is online for it.
func replaceCloudMachine(t *testing.T, e *engine, rs *runtimeSession, s cloudSession, instanceID string, next *machine, cmd string) toolCall {
	t.Helper()
	controller := e.controller()
	controller.deregister(t.Context(), instanceID)
	call, request := waitingCall(t, e, rs, s, cmd)
	if request.Kind != "new" {
		t.Fatalf("the call after the deregistration waits for request %+v; want a new one", request)
	}
	claimed := controller.claimRequest(t.Context(), request.ID, "worker-"+next.name)
	nextID, credentialID := claimed.bootstrapFields(t)
	next.configure(claimed.Bootstrap)
	next.serve("--idle-exit", "15m")
	requireCloudOnline(t, e, next, nextID, credentialID, s)
	return call
}

// driveContentOutlivesAReplacedCloudMachine has a cloud computer's
// command write into a drive that holds a developer's file. The Controller
// then destroys the machine, here by killing its App Server for good, and
// deregisters its instance; the computer's next command runs on a new
// machine claimed for its new request and reads both files.
func driveContentOutlivesAReplacedCloudMachine(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	developer.upload(ctx, driveID, "uploaded.txt", "uploaded")
	first := set.add("first", "first-launcher")
	s := developer.createCloudSession(ctx, "replaced", first.directory("workspace"), readWrite(driveID))
	rs := e.startRuntime().bind(s.sessionID)
	server, instanceID := startClaimedMachine(t, e, rs, first, s)
	if _, written := rs.run(s.computerID, "printf written > "+inDrive(driveID, "written.txt"), 2*time.Minute); written.DriveSync == nil || written.DriveSync.Committed != 1 {
		t.Fatalf("the writing command's drive_sync %+v; want written.txt committed", written.DriveSync)
	}
	if err := server.signal(syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL to the App Server: %v", err)
	}
	<-server.exited
	record.observe("killed the App Server of cloud instance %s for good", instanceID)
	call := replaceCloudMachine(t, e, rs, s, instanceID, set.add("second", "second-launcher"), "cat "+inDrive(driveID, "uploaded.txt")+" "+inDrive(driveID, "written.txt"))
	rs.requireCompleted(call, time.Minute, "uploadedwritten")
	record.observe("after the deregistration the computer's next command ran on the new machine and read uploaded.txt and written.txt")
}

// lastingMountFailuresAreReportedOncePerMachine puts a file at a cloud
// computer's mount point of a drive and starts its App Server again twice,
// then has the Controller deregister the instance and serves the next
// command on a new machine whose mount point is blocked the same way. The
// first machine writes one drive_mount_error over both starts, and the new
// machine one more.
func lastingMountFailuresAreReportedOncePerMachine(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer, controller := e.developer(), e.controller()
	driveID := developer.createDrive(ctx)
	first := set.add("first", "first-launcher")
	s := developer.createCloudSession(ctx, "lasting", first.directory("workspace"), readOnly(driveID))
	rs := e.startRuntime().bind(s.sessionID)
	server, instanceID := startClaimedMachine(t, e, rs, first, s)
	credentialID := controller.readInstance(ctx, instanceID).Connection.CredentialID
	stopMachine(t, controller, server, instanceID)
	if code, out := first.shell("sh", "-c", `rmdir "$1" && touch "$1"`, "sh", first.mountPoint(s.sessionID, driveID)); code != 0 {
		t.Fatalf("replacing the mount point with a file exited %d %s", code, out)
	}
	for start := range 2 {
		server = first.serve("--idle-exit", "15m")
		requireCloudOnline(t, e, first, instanceID, credentialID, s)
		mountErrorsOf(t, e, s.sessionID, driveID, s.computerID, 1)
		record.observe("start %d of the first machine's App Server with a file at the mount point: one drive_mount_error in all", start+1)
		stopMachine(t, controller, server, instanceID)
	}
	second := set.add("second", "second-launcher")
	if code, out := second.shell("sh", "-c", `umask 077 && mkdir -p "$(dirname "$1")" && touch "$1"`, "sh", second.mountPoint(s.sessionID, driveID)); code != 0 {
		t.Fatalf("blocking the new machine's mount point exited %d %s", code, out)
	}
	call := replaceCloudMachine(t, e, rs, s, instanceID, second, "true")
	rs.requireCompleted(call, time.Minute, "")
	mountErrorsOf(t, e, s.sessionID, driveID, s.computerID, 2)
	record.observe("the new machine, its mount point blocked the same way, wrote the second drive_mount_error")
}

// sessionDataStaysUntilTheArchiveUploadsIt keeps a Session idle for a
// minute, restarts its machine's App Server and ends its connection: the
// Session's local data stays through all three. Then a process outside the
// CLI writes a new file and rewrites one that the developer changes
// meanwhile, and the Session is archived with both changes pending. The new
// file is committed, the rewrite is rejected and the drive keeps the
// developer's version, and the machine deletes the Session's local data.
func sessionDataStaysUntilTheArchiveUploadsIt(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	developer.upload(ctx, driveID, "f.txt", "first")
	m := onlineUserMachine(t, e, set, "machine")
	session, computers, rs := driveSession(t, e, "archive", []sessionDrive{readWrite(driveID)}, m)
	if out, _ := rs.run(computers[0], "cat "+inDrive(driveID, "f.txt"), 2*time.Minute); out != "first" {
		t.Fatalf("the first command read %q; want first", out)
	}
	binding := func() string { return *developer.readSessionComputer(ctx, session.ID, computers[0]).BindingID }
	stays := func(after string) {
		t.Helper()
		if !m.sessionData(session.ID) {
			t.Fatalf("the machine holds no data of the Session after %s", after)
		}
		record.observe("the Session's local data stayed after %s", after)
	}
	time.Sleep(time.Minute)
	stays("a minute idle")
	previous := binding()
	m.restart()
	preparedAgain(e, session.ID, computers[0], previous)
	stays("an App Server restart")
	previous = binding()
	loseConnection(e, m)
	preparedAgain(e, session.ID, computers[0], previous)
	stays("a lost connection")

	if code, out := m.shell("sh", "-c", `cd "$1" && printf agent > f.txt && printf new > new.txt`, "sh", m.mountPoint(session.ID, driveID)); code != 0 {
		t.Fatalf("writing f.txt and new.txt outside the CLI exited %d: %s", code, out)
	}
	developer.upload(ctx, driveID, "f.txt", "developer")
	// With no task running, a change made outside a command waits for a
	// sync point; the wait is longer than the CLI's 2-second quiet period.
	time.Sleep(3 * time.Second)
	if puts, commits := e.edge.answered(http.MethodPut, contentRoute(driveID)), e.edge.answered(http.MethodPost, commitRoute(driveID)); len(puts)+len(commits) != 0 {
		t.Fatalf("the CLI sent %d uploads and %d commits before the archive; want both changes pending", len(puts), len(commits))
	}
	developer.archiveSession(ctx, session.ID)
	for deadline := time.Now().Add(3 * time.Minute); m.sessionData(session.ID); time.Sleep(time.Second) {
		if time.Now().After(deadline) {
			t.Fatal("the machine still holds the Session's data three minutes after the archive")
		}
	}
	files := developer.listFiles(ctx, driveID)
	holds := func(path, content string) bool {
		return slices.ContainsFunc(files, func(f driveFile) bool { return f.Path == path && f.ContentSHA256 == sha256Hex(content) })
	}
	if !holds("new.txt", "new") || !holds("f.txt", "developer") || len(files) != 2 {
		t.Fatalf("the drive lists %+v after the archive; want new.txt committed and the developer's f.txt", files)
	}
	record.observe("with f.txt and new.txt written outside any command and the developer's f.txt uploaded meanwhile, the archive committed new.txt, f.txt kept the developer's version, and the machine deleted the Session's data")
}

// throttled passes a request body on at rate bytes per second.
type throttled struct {
	io.ReadCloser
	rate    int64
	started time.Time
	read    int64
}

func (t *throttled) Read(p []byte) (int, error) {
	if due := t.started.Add(time.Duration(t.read * int64(time.Second) / t.rate)); time.Now().Before(due) {
		time.Sleep(time.Until(due))
	}
	n, err := t.ReadCloser.Read(p[:min(len(p), 64<<10)])
	t.read += int64(n)
	return n, err
}

// largeUploadsDoNotHoldCommands has a command write a small file and a
// 500,000,000-byte file into a drive and exit, while the edge passes the
// large upload on at 2 MB per second, so that it takes minutes. The result
// arrives 30 seconds after the exit, plus the commit round trip, with the
// small file committed and the large one uploading; the next command starts
// without waiting; the large file is committed later with its own seq, and a
// command on another machine reads it only after that commit.
func largeUploadsDoNotHoldCommands(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	writer := onlineUserMachine(t, e, set, "writer")
	reader := onlineUserMachine(t, e, set, "reader")
	_, computers, rs := driveSession(t, e, "writer", []sessionDrive{readWrite(driveID)}, writer)
	_, readers, rr := driveSession(t, e, "reader", []sessionDrive{readOnly(driveID)}, reader)
	large := regexp.MustCompile(`^/v1/client/drives/` + driveID + `/content/` + zerosSHA256(500_000_000) + `$`)
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool { return r.Method == http.MethodPut && large.MatchString(r.URL.Path) },
		before: func(_ http.ResponseWriter, r *http.Request) bool {
			r.Body = &throttled{ReadCloser: r.Body, rate: 2_000_000, started: time.Now()}
			return false
		},
	})

	admitted := time.Now()
	call := rs.execInput(computers[0], map[string]any{"cmd": "printf small > " + inDrive(driveID, "small.txt") +
		" && dd if=/dev/zero bs=1000000 count=500 2>/dev/null > " + inDrive(driveID, "large.bin"), "wait_ms": 50000})
	written := rs.terminal(call, 5*time.Minute)
	after := time.Since(admitted) - time.Duration(written.Result.DurationMS)*time.Millisecond
	if after > 35*time.Second || written.DriveSync == nil || written.DriveSync.Committed != 1 || !written.DriveSync.lists(driveID, "large.bin", "uploading") {
		t.Fatalf("the writing command's result came %s after its exit with drive_sync %+v; want small.txt committed and large.bin uploading within 30 seconds plus the commit round trip", after, written.DriveSync)
	}
	record.observe("the result came %s after the command exited, with small.txt committed and large.bin uploading", after.Round(time.Millisecond))
	started := time.Now()
	rs.run(computers[0], "true", time.Minute)
	if took := time.Since(started); took > 10*time.Second {
		t.Fatalf("the next command's result took %s; want it to start without waiting for large.bin", took)
	}
	if out, _ := rr.run(readers[0], "test -e "+inDrive(driveID, "large.bin")+" && echo present || echo absent", time.Minute); out != "absent" {
		t.Fatalf("the reader's command found large.bin %s before its commit", out)
	}
	record.observe("the next command finished %s after it was sent; the reader found no large.bin yet", time.Since(started).Round(time.Millisecond))

	var small, committed driveChange
	for deadline := time.Now().Add(10 * time.Minute); committed.Path == ""; time.Sleep(time.Second) {
		for _, change := range developer.listChanges(ctx, driveID, 0, "") {
			switch change.Path {
			case "small.txt":
				small = change
			case "large.bin":
				committed = change
			}
		}
		if committed.Path == "" && time.Now().After(deadline) {
			t.Fatal("large.bin was not committed within 10 minutes")
		}
	}
	if committed.Seq == small.Seq {
		t.Fatalf("large.bin was committed with small.txt's seq %d; want its own", small.Seq)
	}
	if out, _ := rr.run(readers[0], "wc -c < "+inDrive(driveID, "large.bin"), 5*time.Minute); out != "500000000" {
		t.Fatalf("the reader's command read %q bytes of large.bin after its commit; want 500000000", out)
	}
	uploads := e.edge.answered(http.MethodPut, large)
	record.observe("large.bin was committed %s after the command's result, with seq %d (small.txt %d); its upload took %s; the reader then read 500,000,000 bytes", time.Since(started).Round(time.Second), committed.Seq, small.Seq, uploads[len(uploads)-1].ended.Sub(uploads[len(uploads)-1].at).Round(time.Second))
}

// TestSpentUploadBudgetDelaysOnlyItsWorkspace sets the upload budget to
// 65,536 bytes per minute, the least a request counts, and has a command
// write two one-byte files into a drive and exit. One upload is accepted and
// the other refused with 429 rate_limited and a Retry-After of at most 60;
// the result arrives 30 seconds after the exit, plus the commit round trip,
// with one file committed and the other uploading. Within the minute, an API
// upload of the same workspace is refused with 429 rate_limit_error, while a
// command of another workspace uploads and commits unrefused. The CLI sends
// the refused upload again no sooner than its Retry-After, and that file is
// committed with no tool call and no session.error. It has an Engine of its
// own because the budget is Engine-wide and would slow other rows' uploads.
func TestSpentUploadBudgetDelaysOnlyItsWorkspace(t *testing.T) {
	const otherID = "other_workspace"
	e, set, record := scenario(t, engineOptions{uploadBudget: "65536"})
	ctx := t.Context()
	otherController, otherDeveloper := e.otherWorkspace(otherID)
	otherDrive := otherDeveloper.createDrive(ctx)
	other := onlineUserMachineOf(t, otherController, e, set, "other")
	otherSession, others, ro := driveSessionOf(t, e, otherDeveloper, otherID, "other", []sessionDrive{readWrite(otherDrive)}, nil, other)
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	writer := onlineUserMachine(t, e, set, "writer")
	session, computers, rs := driveSession(t, e, "writer", []sessionDrive{readWrite(driveID)}, writer)

	type refusal struct{ path, code, retryAfter string }
	var mu sync.Mutex
	var refusals []refusal
	puts := regexp.MustCompile(`^/v1/client/drives/(` + driveID + `|` + otherDrive + `)/content/[0-9a-f]{64}$`)
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool { return r.Method == http.MethodPut && puts.MatchString(r.URL.Path) },
		after: func(response *http.Response) error {
			if response.StatusCode != http.StatusTooManyRequests {
				return nil
			}
			body, err := io.ReadAll(response.Body)
			if err != nil {
				return err
			}
			_ = response.Body.Close()
			response.Body = io.NopCloser(bytes.NewReader(body))
			var answer struct {
				Code string `json:"code"`
			}
			_ = json.Unmarshal(body, &answer)
			mu.Lock()
			defer mu.Unlock()
			refusals = append(refusals, refusal{response.Request.URL.Path, answer.Code, response.Header.Get("Retry-After")})
			return nil
		},
	})
	names := map[string]string{sha256Hex("a"): "a.txt", sha256Hex("b"): "b.txt"}

	admitted := time.Now()
	call := rs.execInput(computers[0], map[string]any{"cmd": "printf a > " + inDrive(driveID, "a.txt") + " && printf b > " + inDrive(driveID, "b.txt")})
	written := rs.terminal(call, 2*time.Minute)
	after := time.Since(admitted) - time.Duration(written.Result.DurationMS)*time.Millisecond
	mu.Lock()
	first := slices.Clone(refusals)
	mu.Unlock()
	if len(first) != 1 || first[0].code != string(protocol.ErrorRateLimited) {
		t.Fatalf("the writing command's uploads were refused %+v; want one 429 rate_limited", first)
	}
	retryAfter, err := strconv.Atoi(first[0].retryAfter)
	if err != nil || retryAfter < 1 || retryAfter > 60 {
		t.Fatalf("the refused upload's Retry-After is %q; want 1 to 60 seconds", first[0].retryAfter)
	}
	late := names[first[0].path[strings.LastIndex(first[0].path, "/")+1:]]
	if after < 30*time.Second || after > 35*time.Second || written.DriveSync == nil || written.DriveSync.Committed != 1 || !written.DriveSync.lists(driveID, late, "uploading") {
		t.Fatalf("the writing command's result came %s after its exit with drive_sync %+v; want one file committed and %s uploading 30 seconds plus the commit round trip after it", after, written.DriveSync, late)
	}
	record.observe("the upload of %s was refused with 429 rate_limited and Retry-After %d; the result came %s after the command exited, with the other file committed and %s uploading", late, retryAfter, after.Round(time.Millisecond), late)

	status, headers, answer := developer.uploadFile(ctx, driveID, "api.txt", []byte("c"), nil)
	var apiError struct {
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	_ = json.Unmarshal(answer, &apiError)
	if apiRetry, err := strconv.Atoi(headers.Get("Retry-After")); status != http.StatusTooManyRequests || apiError.Error.Type != "rate_limit_error" || err != nil || apiRetry < 1 || apiRetry > 60 {
		t.Fatalf("the API upload answered %d with Retry-After %q: %s; want 429 rate_limit_error with a Retry-After of 1 to 60", status, headers.Get("Retry-After"), answer)
	}
	if _, envelope := ro.run(others[0], "printf d > "+inDrive(otherDrive, "d.txt"), time.Minute); envelope.DriveSync == nil || envelope.DriveSync.Committed != 1 {
		t.Fatalf("the other workspace's command result has drive_sync %+v; want its file committed", envelope.DriveSync)
	}
	mu.Lock()
	refused := slices.Clone(refusals)
	mu.Unlock()
	if len(refused) != 1 {
		t.Fatalf("uploads were refused %+v after the first; want no refusal of the other workspace", refused[1:])
	}
	record.observe("%s after the exit the API upload of the workspace was refused with 429 rate_limit_error and Retry-After %s; the other workspace's command uploaded and committed its file unrefused", time.Since(admitted).Round(time.Second), headers.Get("Retry-After"))

	for deadline := time.Now().Add(3 * time.Minute); ; time.Sleep(time.Second) {
		if slices.ContainsFunc(developer.listChanges(ctx, driveID, 0, ""), func(c driveChange) bool { return c.Path == late }) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s was not committed within 3 minutes after the result", late)
		}
	}
	content := regexp.MustCompile("^" + regexp.QuoteMeta(first[0].path) + "$")
	attempts := e.edge.answered(http.MethodPut, content)
	if len(attempts) != 2 || attempts[0].status != http.StatusTooManyRequests || attempts[1].status != http.StatusOK && attempts[1].status != http.StatusCreated {
		t.Fatalf("the uploads of %s were %+v; want the refusal and one accepted upload", late, attempts)
	}
	if waited := attempts[1].at.Sub(attempts[0].at); waited < time.Duration(retryAfter)*time.Second {
		t.Fatalf("the CLI sent %s again %s after its refusal; want no sooner than the Retry-After of %d seconds", late, waited, retryAfter)
	}
	if errs := append(developer.sessionErrors(ctx, session.ID), otherDeveloper.sessionErrors(ctx, otherSession.ID)...); len(errs) != 0 {
		t.Fatalf("the Sessions have session.error events %+v; want none", errs)
	}
	record.observe("the CLI sent %s again %s after its refusal; it was accepted and committed with no tool call, and neither Session has a session.error", late, attempts[1].at.Sub(attempts[0].at).Round(time.Millisecond))
}

// acceptedReport is a session-status report the edge saw accepted, with
// when its answer reached the edge.
type acceptedReport struct {
	protocol.SessionStatus
	answered time.Time
}

// acceptedReports is every accepted session-status report that match
// accepts, in arrival order.
func (e *engine) acceptedReports(match func(protocol.SessionStatus) bool) []acceptedReport {
	e.edge.mu.Lock()
	reports := slices.Clone(e.edge.reports)
	e.edge.mu.Unlock()
	var accepted []acceptedReport
	for _, captured := range reports {
		if report, err := protocol.DecodeSessionStatus(captured.body); err == nil && captured.status == http.StatusOK && match(report) {
			accepted = append(accepted, acceptedReport{report, captured.answered})
		}
	}
	return accepted
}

// sessionDrivesSent counts the session.drives messages of sessionID that
// instanceID's client event streams carried.
func (e *engine) sessionDrivesSent(instanceID, sessionID string) int {
	n := 0
	for _, event := range strings.Split(e.edge.streamed(instanceID), "\n\n") {
		if strings.Contains(event, `"session.drives"`) && strings.Contains(event, sessionID) {
			n++
		}
	}
	return n
}

// bindingChangesReachEveryComputerBeforeItsNextCommand removes a
// read_write drive of an idle Session and sets another to read_only, each
// holding a change a process outside the CLI wrote, and sends a command at
// once: first with one computer; then while a second computer's prepared
// report is withheld; then while a second computer's machine is offline,
// which comes back without its working directory, fails its preparation
// with cwd_not_found, and is called once the directory is back. Each
// prepared binding gets one session.drives; both changes are committed
// before its report, and the command starts after it, without the removed
// drive's files and with the other drive read-only; the removed drive refuses that
// binding's upload once Engine stores the report. The withheld binding gets
// its one session.drives after its prepared report is stored; the offline
// machine's computer gets exactly one, after its second preparation's
// prepared report, and each of its two changes is committed or listed in
// that report's lost_changes, with one drive_changes_lost_error for each
// listed drive. No drive_mount_error names a removed drive and no change
// stays pending.
func bindingChangesReachEveryComputerBeforeItsNextCommand(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	a := onlineUserMachine(t, e, set, "a")
	b := onlineUserMachine(t, e, set, "b")
	c := onlineUserMachine(t, e, set, "c")
	type round struct {
		session       sessionObject
		rs            *runtimeSession
		removed, kept string
		computers     []string
	}
	drives := func() (string, string) { return developer.createDrive(ctx), developer.createDrive(ctx) }
	change := func(m *userMachine, sessionID, driveID string) {
		t.Helper()
		if code, out := m.shell("sh", "-c", `printf "$2" > "$1/$2.txt"`, "sh", m.mountPoint(sessionID, driveID), m.name); code != 0 {
			t.Fatalf("writing %s.txt into %s on %s exited %d: %s", m.name, driveID, m.name, code, out)
		}
	}
	committed := func(driveID, path string) bool {
		return slices.ContainsFunc(developer.listChanges(ctx, driveID, 0, ""), func(c driveChange) bool { return c.Path == path })
	}
	// change applies the binding change of r and runs a command on its
	// first computer at once, which must start after that binding's
	// session.drives report and find the removed drive gone and the other
	// read-only. Both of the first computer's changes must be committed
	// before that report, and a replay of its upload to the removed drive
	// after it must be refused.
	var puts sync.Map
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool {
			return r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/client/drives/")
		},
		before: func(_ http.ResponseWriter, r *http.Request) bool {
			body, err := io.ReadAll(r.Body)
			if err == nil {
				r.Body = io.NopCloser(bytes.NewReader(body))
				puts.Store(r.URL.Path, capturedReport{body: body, headers: r.Header.Clone()})
			}
			return false
		},
	})
	bindingChange := func(r round, m *userMachine) acceptedReport {
		t.Helper()
		binding := *developer.readSessionComputer(ctx, r.session.ID, r.computers[0]).BindingID
		if status := developer.removeDrive(ctx, r.session.ID, r.removed); status != http.StatusNoContent {
			t.Fatalf("removing %s answered %d; want 204", r.removed, status)
		}
		if status := developer.setDriveAccess(ctx, r.session.ID, r.kept, "read_only"); status != http.StatusOK {
			t.Fatalf("setting %s read_only answered %d; want 200", r.kept, status)
		}
		out, _ := r.rs.run(r.computers[0], `date +%s%N; test -e "$TETRAL_DRIVES/`+r.removed+`/`+m.name+`.txt" && echo present || echo absent; (printf x > "$TETRAL_DRIVES/`+r.kept+`/x") 2>/dev/null && echo writable || echo read-only`, 3*time.Minute)
		lines := strings.Fields(out)
		started, err := strconv.ParseInt(lines[0], 10, 64)
		if err != nil || len(lines) != 3 || lines[1] != "absent" || lines[2] != "read-only" {
			t.Fatalf("the command after the binding change printed %q; want its start, absent and read-only", out)
		}
		// Each of the two changes may reach the binding as a session.drives
		// of its own; the first report applies the removal, the last both.
		reports := e.acceptedReports(func(s protocol.SessionStatus) bool { return s.Status == "applied" && s.BindingID == binding })
		if len(reports) == 0 || !time.Unix(0, started).After(reports[len(reports)-1].answered) {
			t.Fatalf("binding %s has session.drives reports %+v; want them stored before the command started at %s", binding, reports, time.Unix(0, started))
		}
		for driveID, report := range map[string]acceptedReport{r.removed: reports[0], r.kept: reports[len(reports)-1]} {
			commits := e.edge.answered(http.MethodPost, commitRoute(driveID))
			if !committed(driveID, m.name+".txt") || len(commits) == 0 || commits[len(commits)-1].status != http.StatusOK || !commits[len(commits)-1].ended.Before(report.answered) {
				t.Fatalf("%s's commits %+v; want %s.txt committed before the session.drives report of %s at %s", driveID, commits, m.name, binding, report.answered)
			}
		}
		stored, _ := puts.Load(`/v1/client/drives/` + r.removed + `/content/` + sha256Hex(m.name))
		put, ok := stored.(capturedReport)
		if !ok {
			t.Fatalf("no upload of %s.txt to %s reached the edge", m.name, r.removed)
		}
		status, answer := replay(t, e, http.MethodPut, `/v1/client/drives/`+r.removed+`/content/`+sha256Hex(m.name), put)
		if status < 400 || status >= 500 {
			t.Fatalf("the replayed upload to the removed %s after the report answered %d %s; want a refusal", r.removed, status, answer)
		}
		last := reports[len(reports)-1]
		record.observe("binding %s: both changes committed, then its %d session.drives reports stored, the last at %s; the command started %s later without %s and with %s read-only; the replayed upload to %s answered %d %s", binding, len(reports), last.answered.Format(time.RFC3339Nano), time.Unix(0, started).Sub(last.answered).Round(time.Millisecond), r.removed, r.kept, r.removed, status, answer)
		return last
	}
	var rounds []round

	removed, kept := drives()
	session, computers, rs := driveSession(t, e, "one", []sessionDrive{readWrite(removed), readWrite(kept)}, a)
	first := round{session, rs, removed, kept, computers}
	change(a, session.ID, removed)
	change(a, session.ID, kept)
	bindingChange(first, a)
	rounds = append(rounds, first)

	removed, kept = drives()
	bConnection := controllerConnection(t, e, b.instanceID)
	withheld, stopWithholding := withholdReports(e, func(s protocol.SessionStatus) bool {
		return s.Status == "prepared" && s.ConnectionID == bConnection
	})
	items := []map[string]any{developer.unservedCloudComputer(ctx, "outstanding")}
	for _, m := range []*userMachine{a, b} {
		work := m.directory("outstanding")
		if code, _ := m.shell("mkdir", "-p", work); code != 0 {
			t.Fatalf("mkdir %s on %s exited %d", work, m.name, code)
		}
		items = append(items, map[string]any{"instance_id": m.instanceID, "working_directory": work})
	}
	session = developer.createSessionWithDrives(ctx, developer.createAgent(ctx, "outstanding"), []sessionDrive{readWrite(removed), readWrite(kept)}, items...)
	computers = []string{session.Computers[1].ID, session.Computers[2].ID}
	developer.waitComputer(ctx, session.ID, computers[0], time.Minute, "ready", func(c computerObject) bool { return c.ExecutionState == "ready" })
	select {
	case <-withheld:
	case <-time.After(time.Minute):
		t.Fatal("no prepared report of b reached the edge within a minute")
	}
	second := round{session, e.startRuntime().bind(session.ID), removed, kept, computers}
	change(a, session.ID, removed)
	change(a, session.ID, kept)
	bindingChange(second, a)
	stopWithholding()
	ready := developer.waitComputer(ctx, session.ID, computers[1], 2*time.Minute, "ready", func(c computerObject) bool { return c.ExecutionState == "ready" })
	prepared := e.acceptedReports(func(s protocol.SessionStatus) bool { return s.Status == "prepared" && s.BindingID == *ready.BindingID })
	var applied []acceptedReport
	for deadline := time.Now().Add(time.Minute); len(applied) == 0 && time.Now().Before(deadline); time.Sleep(time.Second) {
		applied = e.acceptedReports(func(s protocol.SessionStatus) bool { return s.Status == "applied" && s.BindingID == *ready.BindingID })
	}
	time.Sleep(5 * time.Second)
	if sent := e.sessionDrivesSent(b.instanceID, session.ID); len(prepared) != 1 || len(applied) != 1 || !applied[0].answered.After(prepared[0].answered) || sent != 1 {
		t.Fatalf("b's binding %s has prepared %+v and session.drives reports %+v, %d session.drives sent; want one session.drives after the stored prepared report", *ready.BindingID, prepared, applied, sent)
	}
	record.observe("b's withheld preparation was stored at %s; its binding %s then got its one session.drives, reported at %s", prepared[0].answered.Format(time.RFC3339Nano), *ready.BindingID, applied[0].answered.Format(time.RFC3339Nano))
	rounds = append(rounds, second)

	removed, kept = drives()
	session, computers, rs = driveSession(t, e, "offline", []sessionDrive{readWrite(removed), readWrite(kept)}, a, c)
	third := round{session, rs, removed, kept, computers}
	for _, m := range []*userMachine{a, c} {
		change(m, session.ID, removed)
		change(m, session.ID, kept)
	}
	sentBefore := e.sessionDrivesSent(c.instanceID, session.ID)
	if err := c.server.signal(syscall.SIGKILL); err != nil {
		t.Fatalf("killing c's App Server: %v", err)
	}
	c.waitOffline(e)
	if code, _ := c.shell("rm", "-rf", c.directory("offline")); code != 0 {
		t.Fatalf("removing c's working directory exited %d", code)
	}
	bindingChange(third, a)
	c.server = c.serve()
	failed := developer.waitComputer(ctx, session.ID, computers[1], 2*time.Minute, "failed with cwd_not_found", func(o computerObject) bool {
		return o.ExecutionState == "failed" && o.Error != nil && o.Error.Code == "cwd_not_found"
	})
	if code, _ := c.shell("mkdir", "-p", c.directory("offline")); code != 0 {
		t.Fatalf("restoring c's working directory exited %d", code)
	}
	rs.run(computers[1], "true", 3*time.Minute)
	again := developer.readSessionComputer(ctx, session.ID, computers[1])
	prepared = e.acceptedReports(func(s protocol.SessionStatus) bool { return s.Status == "prepared" && s.BindingID == *again.BindingID })
	applied = e.acceptedReports(func(s protocol.SessionStatus) bool {
		return s.Status == "applied" && s.SessionID == session.ID && s.BindingID != *developer.readSessionComputer(ctx, session.ID, computers[0]).BindingID
	})
	if sent := e.sessionDrivesSent(c.instanceID, session.ID) - sentBefore; *again.BindingID == *failed.BindingID || len(prepared) != 1 || len(applied) != 1 || applied[0].BindingID != *again.BindingID || !applied[0].answered.After(prepared[0].answered) || sent != 1 {
		t.Fatalf("c's computer after the call: binding %s (failed %s), prepared %+v, session.drives reports %+v, %d session.drives sent; want exactly one session.drives after the second preparation's stored report", *again.BindingID, *failed.BindingID, prepared, applied, sent)
	}
	lost := map[string]protocol.LostChange{}
	for _, entry := range applied[0].LostChanges.Value {
		lost[entry.DriveID] = entry
	}
	for _, driveID := range []string{removed, kept} {
		entry, listed := lost[driveID]
		if listed == committed(driveID, "c.txt") || listed && !slices.Equal(entry.Paths, []string{"c.txt"}) {
			t.Fatalf("c.txt of %s: committed %v, lost_changes entry %+v; want it either committed or listed alone", driveID, committed(driveID, "c.txt"), entry)
		}
	}
	errs := developer.lostChangeErrors(ctx, session.ID)
	if len(errs) != len(lost) {
		t.Fatalf("drive_changes_lost_error events %+v for lost_changes %+v; want one per listed drive", errs, lost)
	}
	for _, err := range errs {
		if _, listed := lost[err.Error.DriveID]; !listed || err.Error.ComputerID != computers[1] {
			t.Fatalf("drive_changes_lost_error %+v; want one of a listed drive for %s", err.Error, computers[1])
		}
	}
	record.observe("c's computer failed its preparation %s with cwd_not_found, was prepared again with binding %s and then got its one session.drives, whose report lists lost_changes %+v; %d drive_changes_lost_error", *failed.BindingID, *again.BindingID, applied[0].LostChanges.Value, len(errs))
	rounds = append(rounds, third)

	for _, r := range rounds {
		for _, err := range developer.sessionErrors(ctx, r.session.ID) {
			if err.Error.Type == "drive_mount_error" && err.Error.DriveID == r.removed {
				t.Fatalf("Session %s has a drive_mount_error of the removed %s: %+v", r.session.ID, r.removed, err)
			}
		}
		for _, computerID := range r.computers {
			if _, envelope := r.rs.run(computerID, "true", 2*time.Minute); envelope.DriveSync != nil && len(envelope.DriveSync.NotSynced) != 0 {
				t.Fatalf("a command on %s lists not_synced %+v; want no change pending", computerID, envelope.DriveSync.NotSynced)
			}
		}
	}
	record.observe("no drive_mount_error names a removed drive, and a last command on every computer lists nothing not synced")
}

// gatedBody passes the first left bytes of a content download, then blocks
// until its download's request ends.
type gatedBody struct {
	io.ReadCloser
	left   int
	ctx    context.Context
	passed func()
}

func (g *gatedBody) Read(p []byte) (int, error) {
	if g.left <= 0 {
		<-g.ctx.Done()
		return 0, g.ctx.Err()
	}
	n, err := g.ReadCloser.Read(p[:min(len(p), g.left)])
	if g.left -= n; g.left <= 0 {
		g.passed()
	}
	return n, err
}

// endedBindingsFailWaitingReadsAndResumeTheDownloadOnDemand holds the
// download of a drive file after its first 1,052,673 bytes while a process
// outside the CLI reads the file with O_DIRECT and another overwrites a block
// past those bytes, then ends the machine's connection. Both fail with EIO,
// as does a new read, while a file read before stays readable. The new
// preparation starts no download; the next read resumes it with a Range
// request from the bytes kept and reads the original content, which the
// failed write left untouched.
func endedBindingsFailWaitingReadsAndResumeTheDownloadOnDemand(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	content := make([]byte, 8<<20)
	for i := range content {
		content[i] = byte(i*7 + i>>13)
	}
	sum := sha256Hex(string(content))
	developer.upload(ctx, driveID, "big.bin", string(content))
	developer.upload(ctx, driveID, "small.txt", "small")
	m := onlineUserMachine(t, e, set, "machine")
	session, computers, rs := driveSession(t, e, "resume", []sessionDrive{readWrite(driveID)}, m)
	if out, _ := rs.run(computers[0], "cat "+inDrive(driveID, "small.txt"), time.Minute); out != "small" {
		t.Fatalf("the first read of small.txt printed %q", out)
	}
	mount, work := m.mountPoint(session.ID, driveID), m.directory("resume")
	binding := *developer.readSessionComputer(ctx, session.ID, computers[0]).BindingID

	const partial = 1<<20 + 4097
	route := regexp.MustCompile(`^/v1/client/drives/` + driveID + `/content/` + sum + `$`)
	var mu sync.Mutex
	var ranges []string
	var gated, holding atomic.Bool
	holding.Store(true)
	passed := make(chan struct{})
	var once sync.Once
	// While holding, the first download passes partial bytes and stalls;
	// the CLI's retries after a stalled attempt are refused.
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool { return r.Method == http.MethodGet && route.MatchString(r.URL.Path) },
		before: func(w http.ResponseWriter, r *http.Request) bool {
			mu.Lock()
			defer mu.Unlock()
			ranges = append(ranges, r.Header.Get("Range"))
			if len(ranges) > 1 && holding.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				return true
			}
			return false
		},
		after: func(response *http.Response) error {
			if !gated.Swap(true) {
				response.Body = &gatedBody{ReadCloser: response.Body, left: partial, ctx: response.Request.Context(), passed: func() { once.Do(func() { close(passed) }) }}
			}
			return nil
		},
	})
	requests := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(ranges)
	}
	// outside runs script on the machine in the background, outside the
	// CLI; its exit status and stderr go to name.exit and name.err.
	outside := func(name, script string) {
		t.Helper()
		if code, out := m.shell("sh", "-c", `(`+script+`; echo $? > "$1/`+name+`.exit") > /dev/null 2> "$1/`+name+`.err" &`, "sh", work); code != 0 {
			t.Fatalf("starting %s exited %d: %s", name, code, out)
		}
	}
	ended := func(name string) (bool, string) {
		code, status := m.shell("cat", work+"/"+name+".exit")
		_, stderr := m.shell("cat", work+"/"+name+".err")
		return code == 0, status + " " + stderr
	}
	outside("read", `dd if="`+mount+`/big.bin" of=/dev/null bs=1M iflag=direct`)
	select {
	case <-passed:
	case <-time.After(time.Minute):
		t.Fatal("the O_DIRECT read started no download within a minute")
	}
	outside("write", `dd if=/dev/zero of="`+mount+`/big.bin" bs=4096 count=1 seek=1024 conv=notrunc`)
	time.Sleep(2 * time.Second)
	for _, name := range []string{"read", "write"} {
		if done, out := ended(name); done {
			t.Fatalf("the %s ended %s while the download was held; want it waiting", name, out)
		}
	}
	record.observe("the edge passed %d bytes of big.bin's download and held the rest; an O_DIRECT read and a block write past them wait", partial)

	endConnection(e, m, func() {
		for _, name := range []string{"read", "write"} {
			for deadline := time.Now().Add(2 * time.Minute); ; time.Sleep(500 * time.Millisecond) {
				done, out := ended(name)
				if done && strings.HasPrefix(out, "1 ") && strings.Contains(out, "Input/output error") {
					break
				}
				if done || time.Now().After(deadline) {
					t.Fatalf("after the connection ended, the waiting %s ended %v: %s; want EIO", name, done, out)
				}
			}
		}
		if code, out := m.shell("sh", "-c", `cat "$1" 2>&1 > /dev/null`, "sh", mount+"/big.bin"); code == 0 || !strings.Contains(out, "Input/output error") {
			t.Fatalf("a new read of big.bin while unbound exited %d: %s; want EIO", code, out)
		}
		if code, out := m.shell("cat", mount+"/small.txt"); code != 0 || out != "small" {
			t.Fatalf("reading the cached small.txt while unbound exited %d %q; want small", code, out)
		}
	})
	record.observe("when the connection ended, the waiting O_DIRECT read and block write failed with EIO, a new read of big.bin failed and the cached small.txt read")

	preparedAgain(e, session.ID, computers[0], binding)
	before := len(requests())
	time.Sleep(3 * time.Second)
	if after := requests(); len(after) != before {
		t.Fatalf("the new preparation sent big.bin downloads %q; want none before a read", after[before:])
	}
	holding.Store(false)
	code, out := m.shell("sh", "-c", `sha256sum < "$1" | cut -d' ' -f1`, "sh", mount+"/big.bin")
	resumed := requests()[before:]
	if code != 0 || out != sum {
		t.Fatalf("reading big.bin after the preparation exited %d with sha256 %s; want %s", code, out, sum)
	}
	start, err := 0, error(nil)
	if len(resumed) == 1 && strings.HasPrefix(resumed[0], "bytes=") && strings.HasSuffix(resumed[0], "-") {
		start, err = strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(resumed[0], "bytes="), "-"))
	}
	if len(resumed) != 1 || err != nil || start <= 0 || start > partial {
		t.Fatalf("the read after the preparation sent downloads with ranges %q; want one from the kept bytes, at most %d", resumed, partial)
	}
	files := developer.listFiles(ctx, driveID)
	if !slices.ContainsFunc(files, func(f driveFile) bool { return f.Path == "big.bin" && f.ContentSHA256 == sum }) {
		t.Fatalf("drives.files.list %+v; want big.bin unchanged", files)
	}
	record.observe("the new preparation started no download; the next read sent one with Range %s and read big.bin's original sha256 %s, which the drive still holds", resumed[0], sum)
}

// TestCacheEvictionFreesSpaceAndKeepsPendingChanges runs a machine whose
// state root is on a volume of about 7 GiB. Three committed files of 256 MiB
// are read in turn, then a process outside the CLI writes a change that no
// command commits. A file outside the state root fills the volume until
// 400 MiB less than 5 GiB is free: the CLI evicts the two files read first,
// which frees 5 GiB, and keeps the last one read. Filled until 1 GiB less
// than 5 GiB is free, it evicts every committed content. An evicted file
// downloads again on read, and the change stays as written. It is no table
// row because on Linux its volume is one that root mounts first
// (TETRAL_SMALL_VOLUME), which no other row needs.
func TestCacheEvictionFreesSpaceAndKeepsPendingChanges(t *testing.T) {
	e, set, record := scenario(t, engineOptions{})
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	const size, gib, mib = 256 << 20, int64(1) << 30, int64(1) << 20
	names := []string{"a.bin", "b.bin", "c.bin"}
	sums := map[string]string{}
	for i, name := range names {
		content := string(bytes.Repeat([]byte{byte('a' + i)}, size))
		developer.upload(ctx, driveID, name, content)
		sums[name] = sha256Hex(content)
	}
	machine, volume := set.addOnSmallVolume("machine", "machine-app")
	m := onlineUserMachineFor(t, e.controller(), e, machine)
	free := func() int64 {
		t.Helper()
		code, out := m.shell("sh", "-c", `df -Pk "$1" | awk 'NR==2 {print $4}'`, "sh", volume)
		kib, err := strconv.ParseInt(out, 10, 64)
		if code != 0 || err != nil {
			t.Fatalf("df on %s exited %d: %s", volume, code, out)
		}
		return kib << 10
	}
	if available := free(); available > 8*gib || available < 6*gib {
		t.Fatalf("the volume has %d bytes free; want a volume of 6 to 8 GiB", available)
	}
	session, computers, rs := driveSession(t, e, "evict", []sessionDrive{readWrite(driveID)}, m)
	for _, name := range names {
		rs.run(computers[0], "cat "+inDrive(driveID, name)+" > /dev/null", 5*time.Minute)
	}
	mount := m.mountPoint(session.ID, driveID)
	pending := string(bytes.Repeat([]byte("p"), 64<<20))
	if code, out := m.shell("sh", "-c", `head -c 67108864 /dev/zero | tr '\0' p > "$1/pending.bin"`, "sh", mount); code != 0 {
		t.Fatalf("writing pending.bin outside the CLI exited %d: %s", code, out)
	}
	downloads := func(name string) int {
		n := 0
		for _, r := range e.edge.answered(http.MethodGet, contentRoute(driveID)) {
			if strings.HasSuffix(r.path, "/"+sums[name]) {
				n++
			}
		}
		return n
	}
	readOutside := func(name, want string) {
		t.Helper()
		if code, out := m.shell("sh", "-c", `if command -v sha256sum > /dev/null; then sha256sum < "$1"; else shasum -a 256 < "$1"; fi | cut -d' ' -f1`, "sh", mount+"/"+name); code != 0 || out != want {
			t.Fatalf("reading %s outside the CLI exited %d with sha256 %q; want %s", name, code, out, want)
		}
	}
	fillTo := func(target int64) {
		t.Helper()
		grow := free() - target
		if code, out := m.shellWithin(10*time.Minute, "sh", "-c", `head -c "$2" /dev/zero >> "$1/filler"`, "sh", volume, strconv.FormatInt(grow, 10)); code != 0 {
			t.Fatalf("growing the filler by %d bytes exited %d: %s", grow, code, out)
		}
	}
	// settled waits until free space stops changing for two eviction
	// intervals and returns it.
	settled := func() int64 {
		t.Helper()
		last := free()
		for still, deadline := 0, time.Now().Add(3*time.Minute); still < 3; still++ {
			time.Sleep(10 * time.Second)
			if now := free(); now != last {
				last, still = now, -1
			}
			if time.Now().After(deadline) {
				t.Fatal("free space did not settle within 3 minutes")
			}
		}
		return last
	}

	fillTo(5*gib - 400*mib)
	if after := settled(); after < 5*gib {
		t.Fatalf("after the fill to 400 MiB below 5 GiB free, %d bytes are free; want eviction to 5 GiB", after)
	}
	before := downloads("c.bin")
	readOutside("c.bin", sums["c.bin"])
	if downloads("c.bin") != before {
		t.Fatal("c.bin, read last, downloaded again after the first eviction; want it kept")
	}
	before = downloads("a.bin")
	readOutside("a.bin", sums["a.bin"])
	if downloads("a.bin") != before+1 {
		t.Fatal("a.bin, read first, was not downloaded again after the first eviction; want it evicted")
	}
	readOutside("pending.bin", sha256Hex(pending))
	record.observe("filled to 400 MiB below 5 GiB free, the CLI evicted the least recently read content until 5 GiB was free: c.bin read from the cache, a.bin downloaded again, pending.bin unchanged")

	fillTo(4 * gib)
	if after := settled(); after >= 5*gib {
		t.Fatalf("after the fill to 1 GiB below 5 GiB free, %d bytes are free; want less than 5 GiB with no committed content left", after)
	}
	for _, name := range names {
		before := downloads(name)
		readOutside(name, sums[name])
		if downloads(name) != before+1 {
			t.Fatalf("%s was not downloaded again after the second eviction; want every committed content evicted", name)
		}
	}
	readOutside("pending.bin", sha256Hex(pending))
	record.observe("filled to 1 GiB below 5 GiB free, the CLI evicted every committed content: each file downloaded again on read; pending.bin, not committed, stayed as written")
}

// macWritesReachLinuxMachines has Session S1 write report.md through a
// Mac's mount; Session S2's next command, on a Linux machine, reads the new
// version.
func macWritesReachLinuxMachines(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	developer.upload(ctx, driveID, "report.md", "draft")
	mac := onlineUserMachine(t, e, set, "mac")
	linux := onlineUserMachineFor(t, e.controller(), e, set.addLinux("linux", "linux-app"))
	_, writes, s1 := driveSession(t, e, "first", []sessionDrive{readWrite(driveID)}, mac)
	_, reads, s2 := driveSession(t, e, "second", []sessionDrive{readWrite(driveID)}, linux)
	if out, _ := s2.run(reads[0], "cat "+inDrive(driveID, "report.md"), 2*time.Minute); out != "draft" {
		t.Fatalf("S2 read %q on Linux; want draft", out)
	}
	_, written := s1.run(writes[0], "printf final > "+inDrive(driveID, "report.md"), 2*time.Minute)
	if sync := written.DriveSync; sync == nil || sync.Committed != 1 {
		t.Fatalf("the Mac command's drive_sync %+v; want report.md committed", written.DriveSync)
	}
	if out, _ := s2.run(reads[0], "cat "+inDrive(driveID, "report.md"), 2*time.Minute); out != "final" {
		t.Fatalf("S2's next command on Linux read %q; want final", out)
	}
	record.observe("S1 wrote report.md on the Mac and its result committed it; S2's next command on the Linux machine read the new version")
}

// noFUSEScript is fusermount3 as a machine without /dev/fuse runs it.
const noFUSEScript = "#!/bin/sh\necho \"fusermount3: fuse device not found, try 'modprobe fuse' first\" >&2\nexit 1\n"

// machinesThatCannotMountPrepareWithoutDrives binds two drives to a
// Session on each of two machines that cannot mount: a Mac whose drives are
// not enabled, and Linux whose fusermount3 fails as it does without
// /dev/fuse. Each machine's ready says it cannot mount, the computer
// prepares without drives, TETRAL_DRIVES is unset in its commands, and one
// drive_mount_error is written per drive. Once drives are enabled on the
// Mac, or FUSE works on Linux, and the App Server connects again, both
// drives mount and no further error is written.
func machinesThatCannotMountPrepareWithoutDrives(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	controller := e.controller()
	one, two := developer.createDrive(ctx), developer.createDrive(ctx)
	developer.upload(ctx, one, "a.txt", "one")
	developer.upload(ctx, two, "b.txt", "two")
	for _, mac := range []bool{true, false} {
		var m *machine
		if mac {
			m = set.add("mac", "mac-app")
		} else {
			m = set.addLinux("linux", "linux-app")
		}
		registered := controller.createUserInstance(ctx, m.name)
		m.withoutDrives = mac
		m.configure(bootstrap(e.serverURL, registered.instance.ID, registered.credential))
		u := &userMachine{machine: m, instanceID: registered.instance.ID, credentialID: registered.credential.ID}
		if mac {
			u.server = m.serve()
		} else {
			noFUSE := m.directory("no-fuse")
			if err := os.MkdirAll(noFUSE, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(noFUSE+"/fusermount3", []byte(noFUSEScript), 0o700); err != nil { //nolint:gosec // an executable the App Server runs
				t.Fatal(err)
			}
			u.server = m.serveWith(map[string]string{"PATH": noFUSE + ":" + os.Getenv("PATH")})
			record.boundary("Linux without /dev/fuse is a fusermount3, first on the App Server's PATH, that fails as fusermount3 does without /dev/fuse; the host keeps its /dev/fuse")
		}
		instance := waitOnline(t, controller, m, registered.instance.ID, registered.credential.ID)
		var capabilities struct {
			Drives struct {
				Mount bool `json:"mount"`
			} `json:"drives"`
		}
		if instance.Connection == nil || json.Unmarshal(instance.Connection.Capabilities, &capabilities) != nil || capabilities.Drives.Mount {
			t.Fatalf("the connection of %s %+v; want ready with drives.mount false", m.name, instance.Connection)
		}
		session, computers, rs := driveSession(t, e, "unmounted-"+m.name, []sessionDrive{readWrite(one), readWrite(two)}, u)
		if out, _ := rs.run(computers[0], `echo "${TETRAL_DRIVES-unset}"`, 2*time.Minute); out != "unset" {
			t.Fatalf("the command on %s printed TETRAL_DRIVES %q; want it unset", m.name, out)
		}
		mountErrors := func(want int) []sessionError {
			deadline := time.Now().Add(30 * time.Second)
			for {
				errs := errorsOfType(e, session.ID, "drive_mount_error")
				if len(errs) >= want || time.Now().After(deadline) {
					return errs
				}
				time.Sleep(500 * time.Millisecond)
			}
		}
		errs := mountErrors(2)
		drives := map[string]bool{}
		for _, err := range errs {
			if err.Error.ComputerID != computers[0] {
				t.Fatalf("drive_mount_error %+v; want computer %s", err, computers[0])
			}
			drives[err.Error.DriveID] = true
		}
		if len(errs) != 2 || !drives[one] || !drives[two] {
			t.Fatalf("drive_mount_errors %+v on %s; want one for each of %s and %s", errs, m.name, one, two)
		}
		record.observe("on %s, ready declared drives.mount false; the computer prepared without drives, TETRAL_DRIVES was unset, and one drive_mount_error was written per drive: %q", m.name, errs[0].Error.Message)

		previous := developer.readSessionComputer(ctx, session.ID, computers[0])
		u.server.stop()
		if mac {
			m.enableDrives()
		}
		u.server = u.serve()
		waitOnline(t, controller, m, registered.instance.ID, registered.credential.ID)
		preparedAgain(e, session.ID, computers[0], *previous.BindingID)
		if out, _ := rs.run(computers[0], `cat "$TETRAL_DRIVES/`+one+`/a.txt" "$TETRAL_DRIVES/`+two+`/b.txt"`, 2*time.Minute); out != "onetwo" {
			t.Fatalf("after enabling, the command on %s read %q; want both drives mounted", m.name, out)
		}
		if errs := mountErrors(3); len(errs) != 2 {
			t.Fatalf("drive_mount_errors %+v on %s after enabling; want still two", errs, m.name)
		}
		record.observe("on %s, with drives enabled and the App Server connected again, the new preparation mounted both drives and the next command read them; no further drive_mount_error", m.name)
	}
}

// drivesVariableNamesTheSessionsDrivesOnEveryPlatform runs ls
// "$TETRAL_DRIVES" and ls "$TETRAL_DRIVES"/<drive_id> in a Session's
// commands on a Mac and on a Linux machine, which also serve another
// Session that binds another drive: both list the Session's one drive and
// its files.
func drivesVariableNamesTheSessionsDrivesOnEveryPlatform(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	listed, other := developer.createDrive(ctx), developer.createDrive(ctx)
	developer.upload(ctx, listed, "a.txt", "a")
	developer.upload(ctx, listed, "b.txt", "b")
	developer.upload(ctx, other, "c.txt", "c")
	mac := onlineUserMachine(t, e, set, "mac")
	linux := onlineUserMachineFor(t, e.controller(), e, set.addLinux("linux", "linux-app"))
	_, computers, rs := driveSession(t, e, "listed", []sessionDrive{readWrite(listed)}, mac, linux)
	driveSession(t, e, "other", []sessionDrive{readWrite(other)}, mac, linux)
	for i, m := range []*userMachine{mac, linux} {
		out, _ := rs.run(computers[i], `ls "$TETRAL_DRIVES"; ls "$TETRAL_DRIVES/`+listed+`"`, 2*time.Minute)
		if out != listed+"\na.txt\nb.txt" {
			t.Fatalf("on %s the command listed %q; want %s with a.txt and b.txt", m.name, out, listed)
		}
	}
	record.observe("on the Mac and on the Linux machine, $TETRAL_DRIVES held only the Session's drive %s, which listed a.txt and b.txt", listed)
}

// macClock is the Mac's local time in the form log show --start takes.
func macClock(m *machine) string {
	m.t.Helper()
	code, now := m.shell("date", "+%Y-%m-%d %H:%M:%S")
	if code != 0 {
		m.t.Fatalf("date on %s exited %d", m.name, code)
	}
	return now
}

// nfsNotResponding is every kernel message since start, a macClock time,
// that an NFS server is not responding.
func nfsNotResponding(m *machine, start string) string {
	m.t.Helper()
	_, out := m.shell("sh", "-c", `/usr/bin/log show --start "$1" --style compact --predicate 'process == "kernel" AND eventMessage CONTAINS[c] "not responding"' | tail -n +2`, "sh", start)
	return out
}

// longDownloadsAndShortPausesEndWithoutErrorOnAMac has a command on a
// Mac read a file whose download the edge passes on at 64,000 bytes per
// second, so that it takes minutes: the read returns the file with no error.
// Then two commands wait for held downloads while the App Server is stopped
// for 20 seconds and one of them is cancelled meanwhile: once the App Server
// runs again and the downloads pass, the cancelled one's process ends and the
// other reads its file. The record lists the kernel's NFS "not responding"
// messages of the run.
func longDownloadsAndShortPausesEndWithoutErrorOnAMac(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	slow := strings.Repeat("a slow download.", 1<<20)
	developer.upload(ctx, driveID, "slow.bin", slow)
	developer.upload(ctx, driveID, "first.txt", "first")
	developer.upload(ctx, driveID, "second.txt", "second")
	slowRoute := regexp.MustCompile(`^/v1/client/drives/` + driveID + `/content/` + sha256Hex(slow) + `$`)
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool { return r.Method == http.MethodGet && slowRoute.MatchString(r.URL.Path) },
		after: func(response *http.Response) error {
			response.Body = &throttled{ReadCloser: response.Body, rate: 64_000, started: time.Now()}
			return nil
		},
	})
	m := onlineUserMachine(t, e, set, "mac")
	_, computers, rs := driveSession(t, e, "downloads", []sessionDrive{readOnly(driveID)}, m)
	start := macClock(m.machine)

	admitted := time.Now()
	read := rs.execInput(computers[0], map[string]any{"cmd": "shasum -a 256 < " + inDrive(driveID, "slow.bin") + " | cut -d' ' -f1", "wait_ms": 50000})
	done := rs.finished(read, 20*time.Minute)
	took := time.Since(admitted)
	if done.Result.ExitCode == nil || *done.Result.ExitCode != 0 || strings.TrimSpace(done.Result.Stdout.Text) != sha256Hex(slow) || done.Result.Stderr.Text != "" || took < 3*time.Minute {
		t.Fatalf("the read of slow.bin ended after %s with %+v; want its hash, no error, after minutes", took, done)
	}
	record.observe("the read of slow.bin (%d bytes at 64,000 bytes per second) returned its hash after %s with nothing on stderr; %d download requests", len(slow), took.Round(time.Second), len(e.edge.answered(http.MethodGet, slowRoute)))

	held := regexp.MustCompile(`^/v1/client/drives/` + driveID + `/content/(` + sha256Hex("first") + `|` + sha256Hex("second") + `)$`)
	release := make(chan struct{})
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool { return r.Method == http.MethodGet && held.MatchString(r.URL.Path) },
		before: func(_ http.ResponseWriter, r *http.Request) bool {
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return false
		},
	})
	// Both calls still wait for their results: a turn end's cancel reaches
	// only such a call. Each read leaves its process ID in <name>.pid.
	waiting := func(name string) toolCall {
		return rs.execInput(computers[0], map[string]any{"cmd": "echo $$ > " + name + ".pid; exec cat " + inDrive(driveID, name), "wait_ms": 50000})
	}
	first, second := waiting("first.txt"), waiting("second.txt")
	for deadline := time.Now().Add(time.Minute); len(e.edge.answered(http.MethodGet, held)) < 2; time.Sleep(200 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the two held downloads did not arrive within a minute")
		}
	}
	if err := m.server.signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	paused := time.Now()
	rs.cancel(second)
	time.Sleep(20*time.Second - time.Since(paused))
	if err := m.server.signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	record.command("SIGSTOP to the App Server for 20 seconds while both reads waited, with the second cancelled meanwhile; then SIGCONT and the downloads released")
	close(release)
	if settled := rs.await(second.use, time.Minute); settled.Settlement != "unknown_outcome" {
		t.Fatalf("the cancelled read settled %+v; want unknown_outcome", settled)
	}
	for deadline := time.Now().Add(time.Minute); ; time.Sleep(time.Second) {
		if out, _ := rs.run(computers[0], "kill -0 $(cat second.txt.pid) 2>/dev/null && echo running || echo ended", time.Minute); out == "ended" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the cancelled read's process still ran a minute after the App Server continued")
		}
	}
	if ended := rs.finished(first, 5*time.Minute); ended.Result.ExitCode == nil || *ended.Result.ExitCode != 0 || ended.Result.Stdout.Text != "first" || ended.Result.Stderr.Text != "" {
		t.Fatalf("the other read ended %+v; want first with no error", ended)
	}
	if out, _ := rs.run(computers[0], "cat "+inDrive(driveID, "second.txt"), 2*time.Minute); out != "second" {
		t.Fatalf("a new read of second.txt returned %q", out)
	}
	record.observe("after the pause the cancelled read had settled unknown_outcome and its process had ended, the other returned first with no error, and a new read returned second")
	messages := nfsNotResponding(m.machine, start)
	record.observe("kernel NFS \"not responding\" messages since %s: %q", start, messages)
	record.boundary("macOS's dialogs show on the Mac's screen: the person at the Mac watches for them; the harness reads only the kernel's log")
}

// remoteChangesReachAMacMount has a command on a Mac list a drive and
// read f.txt, then the developer rewrites f.txt, adds new.txt and deletes
// old.txt: the next command reads the new content and lists new.txt without
// old.txt. A file the command after it writes is committed with that
// command's result.
func remoteChangesReachAMacMount(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	developer.upload(ctx, driveID, "f.txt", "v1")
	developer.upload(ctx, driveID, "old.txt", "old")
	m := onlineUserMachine(t, e, set, "mac")
	session, computers, rs := driveSession(t, e, "remote", []sessionDrive{readWrite(driveID)}, m)
	look := `cat "$TETRAL_DRIVES/` + driveID + `/f.txt"; echo; ls "$TETRAL_DRIVES/` + driveID + `"`
	if out, _ := rs.run(computers[0], look, 2*time.Minute); out != "v1\nf.txt\nold.txt" {
		t.Fatalf("the first command saw %q", out)
	}
	developer.upload(ctx, driveID, "f.txt", "v2")
	developer.upload(ctx, driveID, "new.txt", "new")
	if status := developer.deleteFile(ctx, driveID, "old.txt", "old"); status != http.StatusOK {
		t.Fatalf("deleting old.txt answered %d", status)
	}
	if out, _ := rs.run(computers[0], look, 2*time.Minute); out != "v2\nf.txt\nnew.txt" {
		t.Fatalf("after the remote changes the next command saw %q; want v2 with f.txt and new.txt", out)
	}
	record.observe("after the developer rewrote f.txt, added new.txt and deleted old.txt, the next command on the Mac read v2 and listed f.txt and new.txt")
	_, written := rs.run(computers[0], "printf mine > "+inDrive(driveID, "w.txt"), 2*time.Minute)
	if sync := written.DriveSync; sync == nil || sync.Committed != 1 {
		t.Fatalf("the writing command's drive_sync %+v; want w.txt committed", written.DriveSync)
	}
	changes := developer.listChanges(ctx, driveID, 0, session.ID)
	if len(changes) != 1 || changes[0].Path != "w.txt" || changes[0].Writer.ComputerID != computers[0] {
		t.Fatalf("the Session's change records %+v; want w.txt written by %s", changes, computers[0])
	}
	if status, content := developer.readFile(ctx, driveID, "w.txt"); status != http.StatusOK || content != "mine" {
		t.Fatalf("reading w.txt answered %d %q", status, content)
	}
	record.observe("w.txt, written through the Mac mount, was committed with the command's result, seq %d", changes[0].Seq)
}

// searchingAMacMountMatchesLocalDisk uploads 20,000 files into a drive,
// one in 50 holding "needle", and has a command on a Mac search them with
// rg on the mount, then copy them to local disk and search the copy: both
// searches find the same 400 lines. The record holds both times.
func searchingAMacMountMatchesLocalDisk(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	files := map[string]string{}
	for i := range 20000 {
		content := fmt.Sprintf("file %d\nplain text\n", i)
		if i%50 == 0 {
			content += fmt.Sprintf("needle %d\n", i)
		}
		files[fmt.Sprintf("corpus/%03d/%05d.txt", i/200, i)] = content
	}
	uploaded := time.Now()
	developer.uploadAll(ctx, driveID, files)
	record.observe("uploaded %d files in %s", len(files), time.Since(uploaded).Round(time.Second))
	m := onlineUserMachine(t, e, set, "mac")
	_, computers, rs := driveSession(t, e, "search", []sessionDrive{readOnly(driveID)}, m)
	search := func(cmd string) execEnvelope {
		ended := rs.finished(rs.execInput(computers[0], map[string]any{"cmd": cmd, "wait_ms": 50000}), 30*time.Minute)
		if ended.Result.ExitCode == nil || *ended.Result.ExitCode != 0 {
			t.Fatalf("%q ended %+v", cmd, ended)
		}
		return ended
	}
	corpus := `"$TETRAL_DRIVES/` + driveID + `/corpus"`
	mount := search(`out="$PWD"; cd ` + corpus + ` && /usr/bin/time -p rg --sort path -n needle . > "$out/mount.txt" 2> "$out/mount.time"; tail -n 3 "$out/mount.time"`)
	local := search(`cp -R ` + corpus + ` local && cd local && /usr/bin/time -p rg --sort path -n needle . > ../local.txt 2> ../local.time; tail -n 3 ../local.time`)
	compared, _ := rs.run(computers[0], `cmp mount.txt local.txt && wc -l < mount.txt`, 2*time.Minute)
	if strings.TrimSpace(compared) != "400" {
		t.Fatalf("the two searches compared %q; want the same 400 lines", compared)
	}
	record.observe("rg over the mount and over the local copy found the same 400 lines; on the mount: %q; on local disk: %q", mount.Result.Stdout.Text, local.Result.Stdout.Text)
}

// oneMacStoresADrivesContentOnce has two Sessions on one Mac bind the
// same drive and read its 64 MiB file in turn: the machine's state root grows
// by the file's size at the first read and by less than 4 MiB at the second.
func oneMacStoresADrivesContentOnce(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	content := strings.Repeat("0123456789abcdef", 4<<20)
	developer.upload(ctx, driveID, "shared.bin", content)
	m := onlineUserMachine(t, e, set, "mac")
	_, first, s1 := driveSession(t, e, "first", []sessionDrive{readOnly(driveID)}, m)
	_, second, s2 := driveSession(t, e, "second", []sessionDrive{readOnly(driveID)}, m)
	usage := func() int64 {
		code, out := m.shell("du", "-sk", m.home)
		fields := strings.Fields(out)
		kib, err := strconv.ParseInt(fields[0], 10, 64)
		if code != 0 || len(fields) != 2 || err != nil {
			t.Fatalf("du -sk %s exited %d: %q", m.home, code, out)
		}
		return kib << 10
	}
	read := func(s *runtimeSession, computerID string) {
		// The first read downloads 64 MiB, which can outlast the default wait.
		done := s.finished(s.execInput(computerID, map[string]any{"cmd": "shasum -a 256 < " + inDrive(driveID, "shared.bin") + " | cut -d' ' -f1", "wait_ms": 50000}), 10*time.Minute)
		if done.Result.ExitCode == nil || *done.Result.ExitCode != 0 || strings.TrimSpace(done.Result.Stdout.Text) != sha256Hex(content) {
			t.Fatalf("the read ended %+v; want exit 0 and the content's hash", done)
		}
	}
	before := usage()
	read(s1, first[0])
	afterFirst := usage()
	read(s2, second[0])
	afterSecond := usage()
	if afterFirst-before < int64(len(content)) || afterSecond-afterFirst >= 4<<20 {
		t.Fatalf("the state root used %d bytes, then %d after the first read and %d after the second; want %d more once", before, afterFirst, afterSecond, len(content))
	}
	record.observe("the state root used %d bytes, %d after the first Session's read and %d after the second's", before, afterFirst, afterSecond)
}
