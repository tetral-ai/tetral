package joint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/protocol"
)

// TestDriveTransfer runs the drive transfer scenarios of commits, downloads,
// mount listings and catch-up on one Engine with two computer-hub pods.
func TestDriveTransfer(t *testing.T) {
	scenarioTable(t, engineOptions{hubPods: 2}, needs{},
		tableRow("one_command_commit", oneCommandCommitsItsFilesTogether),
		tableRow("download_retry", downloadsRetryStallsAndFailOnMissingContent),
		tableRow("failed_mount_listing", failedMountListingsLeaveTheDriveUnmountedUntilTheyPass),
		tableRow("file_size_limit", filesOverTheSizeLimitAreRejected),
		tableRow("commit_change_limit", commitsHoldAtMost10000Changes),
		tableRow("fresh_mount_catch_up", freshMountCatchesUpWithCommitsMadeWhileItLists),
		tableRow("catch_up_after_expiry", catchUpOutlivesRemovedVersionsAndExpiredCursors),
		tableRow("cleanup_pass", driveCleanupPublishesRetainedStorageAndCursorExpiry).only("linux"),
		tableRow("commit_before_dispatch", commandsReadCommitsMadeBeforeTheirDispatch),
	)
}

// waitStreamed waits up to timeout until instanceID's client event streams
// carried text.
func waitStreamed(t *testing.T, e *engine, instanceID, text string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !strings.Contains(e.edge.streamed(instanceID), text) {
		if time.Now().After(deadline) {
			t.Fatalf("instance %s's event streams did not carry %s within %s", instanceID, text, timeout)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// oneCommandCommitsItsFilesTogether runs a command that writes two files
// into a drive. The CLI sends the two contents and then one commit; both
// change records carry that commit's seq, and the drive's other machine
// receives drive.changed with it.
func oneCommandCommitsItsFilesTogether(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	writer, reader := onlineUserMachine(t, e, set, "writer"), onlineUserMachine(t, e, set, "reader")
	session, computers, rs := driveSession(t, e, "together", []sessionDrive{readWrite(driveID)}, writer, reader)

	_, envelope := rs.run(computers[0], "printf one > "+inDrive(driveID, "a.txt")+"; printf two > "+inDrive(driveID, "b.txt"), 2*time.Minute)
	if sync := envelope.DriveSync; sync == nil || sync.Committed != 2 || len(sync.NotSynced) != 0 || len(sync.Rejected) != 0 {
		t.Fatalf("drive_sync %+v; want two files committed", sync)
	}
	puts := e.edge.answered(http.MethodPut, contentRoute(driveID))
	commits := e.edge.answered(http.MethodPost, commitRoute(driveID))
	if len(puts) != 2 || len(commits) != 1 {
		t.Fatalf("the CLI sent %d content PUTs and %d commits; want 2 and 1", len(puts), len(commits))
	}
	for _, put := range puts {
		if put.status != http.StatusOK && put.status != http.StatusCreated || put.ended.After(commits[0].at) {
			t.Fatalf("content PUT %+v; want accepted before the commit arrived at %s", put, commits[0].at)
		}
	}
	changes := developer.listChanges(ctx, driveID, 0, "")
	if len(changes) != 2 || changes[0].Seq != changes[1].Seq || changes[0].Path != "a.txt" || changes[1].Path != "b.txt" {
		t.Fatalf("change records %+v; want a.txt and b.txt with one seq", changes)
	}
	for _, change := range changes {
		if change.Writer.Type != "session_actor" || change.Writer.SessionID != session.ID || change.Writer.ComputerID != computers[0] {
			t.Fatalf("change record %+v; want written by Session %s on computer %s", change, session.ID, computers[0])
		}
	}
	record.observe("the command's result committed 2 files; the CLI sent 2 content PUTs, both answered before its one commit; the change records of a.txt and b.txt carry seq %d", changes[0].Seq)
	waitStreamed(t, e, reader.instanceID, fmt.Sprintf(`"drive_id":%q,"seq":%d`, driveID, changes[0].Seq), 30*time.Second)
	record.observe("the reader machine's event stream carried drive.changed for %s with seq %d", driveID, changes[0].Seq)
}

// commandsReadCommitsMadeBeforeTheirDispatch has Session S1 commit a
// file that Session S2 reads on another machine: S2's next command, dispatched
// after the commit, reads the new content, and again after S2's stream moved
// to another computer-hub pod.
func commandsReadCommitsMadeBeforeTheirDispatch(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	developer.upload(ctx, driveID, "f.txt", "v1")
	writer := onlineUserMachine(t, e, set, "writer")
	writerPod := lastStream(t, e, writer.instanceID).backend
	// The reader's stream goes to the pod the writer does not use.
	e.edge.setDown(writerPod, true)
	reader := onlineUserMachine(t, e, set, "reader")
	e.edge.setDown(writerPod, false)
	readerPod := lastStream(t, e, reader.instanceID).backend
	if readerPod == writerPod {
		t.Fatalf("both streams on %s; want one on each pod", readerPod)
	}
	_, writes, s1 := driveSession(t, e, "first", []sessionDrive{readWrite(driveID)}, writer)
	_, reads, s2 := driveSession(t, e, "second", []sessionDrive{readWrite(driveID)}, reader)
	if out, _ := s2.run(reads[0], "cat "+inDrive(driveID, "f.txt"), 2*time.Minute); out != "v1" {
		t.Fatalf("S2 read %q; want v1", out)
	}
	s1.run(writes[0], "printf v2 > "+inDrive(driveID, "f.txt"), 2*time.Minute)
	if out, _ := s2.run(reads[0], "cat "+inDrive(driveID, "f.txt"), 2*time.Minute); out != "v2" {
		t.Fatalf("S2's next command read %q; want v2", out)
	}
	record.observe("S1 committed v2 on %s's stream pod %s; S2's next command on %s (pod %s) read v2", writer.name, writerPod, reader.name, readerPod)

	streams := len(e.edge.eventsRequests(reader.instanceID))
	e.killHub(readerPod)
	moved := e.edge.waitEvents(t, reader.instanceID, streams+1, 2*time.Minute, "a stream on the other pod", func(r eventsRequest) bool { return r.status == http.StatusOK })
	if moved.backend != writerPod {
		t.Fatalf("the reader's new stream is on %s; want %s", moved.backend, writerPod)
	}
	reader.waitStatus(time.Minute, "online", func(status map[string]any) bool { return statusField(status, "connection_state") == "online" })
	s1.run(writes[0], "printf v3 > "+inDrive(driveID, "f.txt"), 2*time.Minute)
	if out, _ := s2.run(reads[0], "cat "+inDrive(driveID, "f.txt"), 2*time.Minute); out != "v3" {
		t.Fatalf("S2's next command after its stream moved read %q; want v3", out)
	}
	record.observe("after killing %s the reader's stream moved to %s; S1 committed v3 and S2's next command read v3", readerPod, moved.backend)
}

// lastStream is instanceID's latest client event request at the edge.
func lastStream(t *testing.T, e *engine, instanceID string) eventsRequest {
	t.Helper()
	requests := e.edge.eventsRequests(instanceID)
	if len(requests) == 0 {
		t.Fatalf("instance %s has no event stream", instanceID)
	}
	return requests[len(requests)-1]
}

// stalledBody yields its first bytes, then nothing until stall has passed
// or the request has ended, then the rest.
type stalledBody struct {
	io.ReadCloser
	ctx     context.Context
	first   int
	stall   time.Duration
	read    int
	stalled bool
}

func (b *stalledBody) Read(p []byte) (int, error) {
	if b.read >= b.first && !b.stalled {
		b.stalled = true
		select {
		case <-time.After(b.stall):
		case <-b.ctx.Done():
			return 0, b.ctx.Err()
		}
	}
	if !b.stalled && len(p) > b.first-b.read {
		p = p[:b.first-b.read]
	}
	n, err := b.ReadCloser.Read(p)
	b.read += n
	return n, err
}

// downloadsRetryStallsAndFailOnMissingContent stalls the first download
// of a file for longer than 10 seconds after its first bytes: the CLI drops
// that attempt and tries again, and the read returns the whole file. A
// download answered 404 content_not_found fails its read at once.
func downloadsRetryStallsAndFailOnMissingContent(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	stalled := strings.Repeat("0123456789abcdef", 16384)
	developer.upload(ctx, driveID, "stalled.txt", stalled)
	developer.upload(ctx, driveID, "missing.txt", "missing")
	stalledRoute := regexp.MustCompile(`^/v1/client/drives/` + driveID + `/content/` + sha256Hex(stalled) + `$`)
	missingRoute := regexp.MustCompile(`^/v1/client/drives/` + driveID + `/content/` + sha256Hex("missing") + `$`)
	var stalls atomic.Int32
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool {
			return r.Method == http.MethodGet && stalledRoute.MatchString(r.URL.Path) && stalls.Add(1) == 1
		},
		after: func(response *http.Response) error {
			response.Body = &stalledBody{ReadCloser: response.Body, ctx: response.Request.Context(), first: 4096, stall: 15 * time.Second}
			return nil
		},
	})
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool { return r.Method == http.MethodGet && missingRoute.MatchString(r.URL.Path) },
		before: func(w http.ResponseWriter, _ *http.Request) bool {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write(protocol.Encode(protocol.ErrorBody{Code: protocol.ErrorContentNotFound, Message: "no kept version of this drive names this content"}))
			return true
		},
	})
	m := onlineUserMachine(t, e, set, "reader")
	_, computers, rs := driveSession(t, e, "downloads", []sessionDrive{readOnly(driveID)}, m)

	// The stall and the retry outlast the default 10-second wait.
	read := rs.execInput(computers[0], map[string]any{"cmd": "sha256sum < " + inDrive(driveID, "stalled.txt"), "wait_ms": 50000})
	if envelope := rs.terminal(read, 3*time.Minute); envelope.Result.ExitCode == nil || *envelope.Result.ExitCode != 0 || !strings.HasPrefix(envelope.Result.Stdout.Text, sha256Hex(stalled)) {
		t.Fatalf("the read of stalled.txt ended %+v; want exit 0 hashing %s", envelope, sha256Hex(stalled))
	}
	attempts := e.edge.answered(http.MethodGet, stalledRoute)
	if len(attempts) < 2 || attempts[1].at.Sub(attempts[0].at) < 10*time.Second {
		t.Fatalf("download attempts of stalled.txt %+v; want a second one after the first stalled 10 seconds", attempts)
	}
	record.observe("the first download of stalled.txt stalled after 4096 bytes; the CLI tried again %s later and the read returned all %d bytes", attempts[1].at.Sub(attempts[0].at).Round(time.Millisecond), len(stalled))

	out, _ := rs.run(computers[0], "start=$(date +%s%N); cat "+inDrive(driveID, "missing.txt")+" 2>&1; echo \"exit=$? ms=$(( ($(date +%s%N) - start) / 1000000 ))\"", 2*time.Minute)
	var exit, took int
	if _, err := fmt.Sscanf(out[strings.LastIndex(out, "exit="):], "exit=%d ms=%d", &exit, &took); err != nil || exit == 0 || took > 5000 || !strings.Contains(out, "Input/output error") {
		t.Fatalf("the read of missing.txt wrote %q; want an I/O error within 5 seconds", out)
	}
	downloads := e.edge.answered(http.MethodGet, missingRoute)
	spans := make([]string, 0, len(downloads))
	for _, download := range downloads {
		spans = append(spans, download.at.Sub(downloads[0].at).Round(time.Millisecond).String())
	}
	record.observe("the downloads of missing.txt, at %v from the first, answered 404 content_not_found and its read failed after %d ms: %q", spans, took, out)
}

// failedMountListingsLeaveTheDriveUnmountedUntilTheyPass refuses a first
// mount's file listing with 403 drive_access_denied, then alters the next
// check's change listing so that it fails the schema, then passes every
// answer: the drive stays unmounted while the Session's other drive and its
// commands go on, each failure writes one drive_mount_error, and the third
// check mounts the drive.
func failedMountListingsLeaveTheDriveUnmountedUntilTheyPass(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	failing, other := developer.createDrive(ctx), developer.createDrive(ctx)
	developer.upload(ctx, failing, "f.txt", "failing")
	developer.upload(ctx, other, "g.txt", "other")
	filesRoute := regexp.MustCompile(`^/v1/client/drives/` + failing + `/files$`)
	changesRoute := regexp.MustCompile(`^/v1/client/drives/` + failing + `/changes$`)
	// 0 refuses the file listing, 1 alters the change listing, 2 passes.
	var stage atomic.Int32
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool {
			return r.Method == http.MethodGet && (stage.Load() == 0 && filesRoute.MatchString(r.URL.Path) || stage.Load() == 1 && changesRoute.MatchString(r.URL.Path))
		},
		before: func(w http.ResponseWriter, _ *http.Request) bool {
			if stage.Load() != 0 {
				return false
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write(protocol.Encode(protocol.ErrorBody{Code: protocol.ErrorDriveAccessDenied, Message: "this binding may not use this drive"}))
			return true
		},
		after: func(response *http.Response) error {
			_ = response.Body.Close()
			body := []byte(`{"changes":"none"}`)
			response.Body = io.NopCloser(bytes.NewReader(body))
			response.ContentLength = int64(len(body))
			response.Header.Set("Content-Length", strconv.Itoa(len(body)))
			return nil
		},
	})
	m := onlineUserMachine(t, e, set, "machine")
	session, computers, rs := driveSession(t, e, "mounts", []sessionDrive{readWrite(failing), readWrite(other)}, m)
	// A drive's listings need a prepared binding, so they follow the
	// preparation's report; their failure is reported at the next check.
	refused := func() []edgeRequest {
		return slices.DeleteFunc(e.edge.answered(http.MethodGet, filesRoute), func(r edgeRequest) bool { return r.status != http.StatusForbidden })
	}
	for deadline := time.Now().Add(30 * time.Second); len(refused()) == 0; time.Sleep(200 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("the edge refused no file listing of %s within 30 seconds of the preparation", failing)
		}
	}
	if errs := errorsOfType(e, session.ID, "drive_mount_error"); len(errs) != 0 {
		t.Fatalf("drive_mount_error %+v before the next check; want none", errs)
	}

	stage.Store(1)
	listings := len(e.edge.answered(http.MethodGet, changesRoute))
	out, second := rs.run(computers[0], "cat "+inDrive(other, "g.txt")+"; echo; cat "+inDrive(failing, "f.txt")+" 2>/dev/null || echo unavailable", 2*time.Minute)
	if out != "other\nunavailable" {
		t.Fatalf("the second check's command wrote %q; want the other drive's file and %s unavailable", out, failing)
	}
	if sync := second.DriveSync; sync == nil || len(sync.Unmounted) != 2 || sync.Unmounted[0].DriveID != failing || sync.Unmounted[1].DriveID != failing || sync.Unmounted[0].Error == sync.Unmounted[1].Error {
		t.Fatalf("the second check's drive_sync %+v; want %s unmounted with the refused file listing and the failed change listing", second.DriveSync, failing)
	}
	if len(e.edge.answered(http.MethodGet, changesRoute)) == listings {
		t.Fatalf("the second check sent no change listing of %s", failing)
	}
	mountErrorsOf(t, e, session.ID, failing, computers[0], 2)

	stage.Store(2)
	out, third := rs.run(computers[0], "cat "+inDrive(failing, "f.txt"), 2*time.Minute)
	if out != "failing" || third.DriveSync != nil && len(third.DriveSync.Unmounted) != 0 {
		t.Fatalf("the third check's command wrote %q with drive_sync %+v; want f.txt read from the mounted drive", out, third.DriveSync)
	}
	mountErrorsOf(t, e, session.ID, failing, computers[0], 2)
	record.observe("the file listing of %s after the preparation was answered 403 and the next check's change listing failed the schema; that check's result listed both %+v and two drive_mount_error were written while %s and the commands went on; the third check mounted %s", failing, second.DriveSync.Unmounted, other, failing)
}

// zerosSHA256 is the SHA-256 of n zero bytes, in hex.
func zerosSHA256(n int64) string {
	hash := sha256.New()
	block := make([]byte, 1<<20)
	for ; n > 0; n -= int64(len(block)) {
		hash.Write(block[:min(n, int64(len(block)))])
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// filesOverTheSizeLimitAreRejected has a command write a file of
// 500,000,001 bytes and one of 500,000,000 bytes into a drive: the first
// file's upload is answered 413 file_too_large and the agent gets it as a
// limit rejection, in the command's result or, when the rejection came
// before the command's sync point, in a drive sync report of its task, which
// on a Mac, where no change records its task, names no task; the second is
// accepted and committed.
func filesOverTheSizeLimitAreRejected(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	m := onlineUserMachine(t, e, set, "machine")
	session, computers, rs := driveSession(t, e, "sizes", []sessionDrive{readWrite(driveID)}, m)
	overRoute := regexp.MustCompile(`^/v1/client/drives/` + driveID + `/content/` + zerosSHA256(500_000_001) + `$`)
	maxRoute := regexp.MustCompile(`^/v1/client/drives/` + driveID + `/content/` + zerosSHA256(500_000_000) + `$`)
	reports := driveSyncReports(e)

	write := "dd if=/dev/zero bs=1000000 count=500 2>/dev/null > " + inDrive(driveID, "over.bin") + " && head -c 1 /dev/zero >> " + inDrive(driveID, "over.bin") +
		" && dd if=/dev/zero bs=1000000 count=500 2>/dev/null > " + inDrive(driveID, "max.bin")
	call := rs.exec(computers[0], write)
	envelope := rs.finished(call, 10*time.Minute)
	if envelope.Result.ExitCode == nil || *envelope.Result.ExitCode != 0 || envelope.DriveSync == nil {
		t.Fatalf("the writing command ended %+v; want exit 0 with a drive_sync", envelope)
	}
	over := e.edge.answered(http.MethodPut, overRoute)
	if len(over) != 1 || over[0].status != http.StatusRequestEntityTooLarge {
		t.Fatalf("uploads of over.bin %+v; want one answered 413", over)
	}
	limit := func(sync driveSync) bool {
		return slices.ContainsFunc(sync.Rejected, func(r rejectedChange) bool {
			return strings.HasSuffix(r.Path, "/drives/"+driveID+"/over.bin") && r.Change == "write" && r.Reason == "limit" && r.Rule != ""
		})
	}
	where := "the command's result"
	if !limit(*envelope.DriveSync) {
		where = "a drive sync report of the command's task"
		task := call.taskID
		if set.in.machineOS == "macos" {
			where, task = "a drive sync report of the Session with no task", ""
		}
		if !slices.ContainsFunc(reports(), func(r driveSyncReport) bool {
			return r.SessionID == session.ID && r.TaskID == task && limit(r.DriveSync)
		}) {
			t.Fatalf("the result's drive_sync %+v and drive sync reports %+v; want over.bin rejected as a limit with a rule", envelope.DriveSync, reports())
		}
	}
	for deadline := time.Now().Add(5 * time.Minute); ; time.Sleep(time.Second) {
		if changes := developer.listChanges(ctx, driveID, 0, ""); len(changes) == 1 && changes[0].Path == "max.bin" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("max.bin was not committed within 5 minutes")
		}
	}
	accepted := slices.DeleteFunc(e.edge.answered(http.MethodPut, maxRoute), func(r edgeRequest) bool { return r.status != http.StatusOK && r.status != http.StatusCreated })
	if len(accepted) == 0 {
		t.Fatal("no upload of max.bin was accepted")
	}
	files := developer.listFiles(ctx, driveID)
	if len(files) != 1 || files[0].Path != "max.bin" || files[0].SizeBytes != 500_000_000 {
		t.Fatalf("the drive lists %+v; want max.bin of 500,000,000 bytes", files)
	}
	record.observe("the upload of over.bin was answered 413 after %s and %s rejected it as a limit; the upload of max.bin was answered %d after %s and max.bin was committed with 500,000,000 bytes", over[0].ended.Sub(over[0].at).Round(time.Millisecond), where, accepted[0].status, accepted[0].ended.Sub(accepted[0].at).Round(time.Millisecond))
}

// commitsHoldAtMost10000Changes has a process outside the CLI write
// 10,000 files into a drive, then delete 5,000 of them and write 5,001
// others, each time before a command whose sync point covers them all, and
// then sends a commit of 10,001 changes with the machine's own credential
// and binding. The first sync point commits all 10,000 files at once; the
// second commits in two commits with consecutive seqs, the first holding the
// deletions and then the writes in path order; the direct commit gets 413
// request_too_large. The changes are made outside any command, and uploaded
// before the sync point, because a command writing that many files outlasts
// its first report, so that its later writes are background writes committed
// as they upload, and a sync point's 30-second wait is too short to upload
// them all.
func commitsHoldAtMost10000Changes(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	m := onlineUserMachine(t, e, set, "machine")
	session, computers, rs := driveSession(t, e, "commits", []sessionDrive{readWrite(driveID)}, m)
	// The machine's own credential and binding, for the direct commit.
	commitPath := regexp.MustCompile(`^/v1/client/drives/` + driveID + `/commits$`)
	t.Cleanup(func() {
		var answers []string
		for _, r := range e.edge.answered(http.MethodPost, commitPath) {
			answers = append(answers, fmt.Sprintf("%d after %s", r.status, r.ended.Sub(r.at).Round(10*time.Millisecond)))
		}
		record.observe("the drive's commit requests were answered %s (the CLI waits 10 seconds for an answer)", strings.Join(answers, ", "))
	})
	var authorization atomic.Pointer[string]
	var commitBody atomic.Pointer[[]byte]
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool { return r.Method == http.MethodPost && commitPath.MatchString(r.URL.Path) },
		before: func(_ http.ResponseWriter, r *http.Request) bool {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				return false
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			header := r.Header.Get("Authorization")
			authorization.Store(&header)
			commitBody.Store(&body)
			return false
		},
	})
	rs.runInBackground(computers[0])
	// syncAfter has script upload n contents outside the CLI, then runs a
	// command and returns that command's drive_sync.
	syncAfter := func(script string, n int) driveSync {
		t.Helper()
		uploadOutside(e, m, session.ID, driveID, script, n)
		envelope := rs.finished(rs.exec(computers[0], "true"), 10*time.Minute)
		if envelope.Result.ExitCode == nil || *envelope.Result.ExitCode != 0 || envelope.DriveSync == nil {
			t.Fatalf("the command after %q ended %+v; want exit 0 with a drive_sync", script, envelope)
		}
		return *envelope.DriveSync
	}
	commits := func() int { return len(e.edge.answered(http.MethodPost, commitPath)) }

	first := syncAfter(`for i in $(seq -w 0 9999); do printf f$i > f$i; done`, 10000)
	changes := developer.listChanges(ctx, driveID, 0, "")
	if first.Committed != 10000 || len(first.NotSynced) != 0 || len(first.Rejected) != 0 || commits() != 1 || len(changes) != 10000 || changes[0].Seq != changes[9999].Seq {
		t.Fatalf("the first sync point's drive_sync %+v, %d commits and %d change records; want 10,000 files in one commit", first, commits(), len(changes))
	}
	firstSeq := changes[0].Seq

	second := syncAfter(`rm f[0-4]* && for i in $(seq -w 0 5000); do printf g$i > g$i; done`, 5001)
	changes = developer.listChanges(ctx, driveID, firstSeq, "")
	if second.Committed != 5001 || second.Deleted != 5000 || len(second.NotSynced) != 0 || len(second.Rejected) != 0 || commits() != 3 || len(changes) != 10001 {
		t.Fatalf("the second sync point's drive_sync %+v, %d commits and %d change records; want 5,000 deletions and 5,001 writes in two commits", second, commits()-1, len(changes))
	}
	for i, change := range changes {
		want := driveChange{Seq: firstSeq + 1, Index: i, Operation: "deleted", Path: fmt.Sprintf("f%04d", i)}
		switch {
		case i >= 10000:
			want = driveChange{Seq: firstSeq + 2, Index: 0, Operation: "created", Path: "g5000"}
		case i >= 5000:
			want.Operation, want.Path = "created", fmt.Sprintf("g%04d", i-5000)
		}
		if change.Seq != want.Seq || change.Index != want.Index || change.Operation != want.Operation || change.Path != want.Path {
			t.Fatalf("change record %d is %+v; want seq %d index %d %s %s", i, change, want.Seq, want.Index, want.Operation, want.Path)
		}
	}
	record.observe("the first sync point committed 10,000 files in one commit at seq %d; the second committed seq %d with 5,000 deletions and then 5,000 writes in path order, and seq %d with g5000", firstSeq, firstSeq+1, firstSeq+2)

	header, body := authorization.Load(), commitBody.Load()
	var sent struct {
		ConnectionID string `json:"connection_id"`
		BindingID    string `json:"binding_id"`
	}
	if header == nil || json.Unmarshal(*body, &sent) != nil {
		t.Fatal("no commit of the machine was captured")
	}
	direct := map[string]any{"protocol_version": 1, "connection_id": sent.ConnectionID, "binding_id": sent.BindingID}
	writes := make([]map[string]any, 10001)
	for i := range writes {
		writes[i] = map[string]any{"path": fmt.Sprintf("h%05d", i), "change": "write", "base_sha256": nil, "content_sha256": sha256Hex("g5000"), "executable": false}
	}
	direct["changes"] = writes
	post, err := http.NewRequestWithContext(ctx, http.MethodPost, e.serverURL+"/v1/client/drives/"+driveID+"/commits", strings.NewReader(mustJSON(t, direct)))
	if err != nil {
		t.Fatal(err)
	}
	post.Header.Set("Authorization", *header)
	post.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(post)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	answer, _ := io.ReadAll(response.Body)
	var refused protocol.ErrorBody
	if response.StatusCode != http.StatusRequestEntityTooLarge || json.Unmarshal(answer, &refused) != nil || refused.Code != protocol.ErrorRequestTooLarge {
		t.Fatalf("a direct commit of 10,001 changes was answered %d %s; want 413 request_too_large", response.StatusCode, answer)
	}
	if after := developer.listChanges(ctx, driveID, firstSeq+2, ""); len(after) != 0 {
		t.Fatalf("change records after the direct commit %+v; want none", after)
	}
	record.observe("a direct commit of 10,001 changes with the machine's credential and binding was answered 413 %s", answer)
}

// driveListing writes the path and content SHA-256 of every file of
// driveID's mount, in byte order of path, to listing.txt in the computer's
// working directory, and prints how many it wrote.
func driveListing(driveID string) string {
	return `out="$PWD/listing.txt"; h() { if command -v sha256sum >/dev/null 2>&1; then sha256sum; else shasum -a 256; fi | cut -d' ' -f1; }; ` +
		`cd "$TETRAL_DRIVES/` + driveID + `" && find . -type f | LC_ALL=C sort | while IFS= read -r f; do printf '%s %s\n' "${f#./}" "$(h < "$f")"; done > "$out" && wc -l < "$out"`
}

// requireListingMatches checks that the listing.txt driveListing wrote in
// work on m equals drives.files.list of driveID.
func requireListingMatches(t *testing.T, e *engine, m *userMachine, work, driveID string) int {
	t.Helper()
	code, listed := m.shell("cat", work+"/listing.txt")
	if code != 0 {
		t.Fatalf("reading the listing on %s exited %d", m.name, code)
	}
	var want strings.Builder
	files := e.developer().listFiles(t.Context(), driveID)
	for _, file := range files {
		fmt.Fprintf(&want, "%s %s\n", file.Path, file.ContentSHA256)
	}
	// m.shell trims its output, so the lines are compared, not the text.
	got, wanted := strings.Split(strings.TrimSpace(listed), "\n"), strings.Split(strings.TrimSpace(want.String()), "\n")
	slices.Sort(got)
	slices.Sort(wanted)
	if !slices.Equal(got, wanted) {
		onlyMount, onlyDrive := lineDifference(got, wanted), lineDifference(wanted, got)
		t.Fatalf("the mount lists %d files and drives.files.list %d; only in the mount %q, only in drives.files.list %q", len(got), len(wanted), onlyMount[:min(len(onlyMount), 5)], onlyDrive[:min(len(onlyDrive), 5)])
	}
	return len(files)
}

// lineDifference is the lines of a that b lacks, in a's order.
func lineDifference(a, b []string) []string {
	in := make(map[string]bool, len(b))
	for _, line := range b {
		in[line] = true
	}
	var only []string
	for _, line := range a {
		if !in[line] {
			only = append(only, line)
		}
	}
	return only
}

// freshMountCatchesUpWithCommitsMadeWhileItLists fills a drive with
// 2,100 files and mounts it on a machine whose list pages the edge holds two
// seconds each, while a random sequence of uploads, rewrites and deletions
// runs from before the mount until after its last list page. Once the
// sequence ends, the mount lists exactly the paths and contents of
// drives.files.list.
func freshMountCatchesUpWithCommitsMadeWhileItLists(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	contents := map[string]string{}
	for i := range 2100 {
		contents[fmt.Sprintf("f%04d", i)] = fmt.Sprintf("v0 f%04d", i)
	}
	developer.uploadAll(ctx, driveID, contents)
	m := onlineUserMachine(t, e, set, "machine")
	e.edge.delay("/v1/client/drives/"+driveID+"/files", 2*time.Second)
	var listed atomic.Bool
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool {
			return r.Method == http.MethodGet && r.URL.Path == "/v1/client/drives/"+driveID+"/files"
		},
		after: func(response *http.Response) error {
			body, err := io.ReadAll(response.Body)
			if err != nil {
				return err
			}
			_ = response.Body.Close()
			response.Body = io.NopCloser(bytes.NewReader(body))
			if response.StatusCode == http.StatusOK && bytes.Contains(body, []byte(`"has_more":false`)) {
				listed.Store(true)
			}
			return nil
		},
	})
	seed := uint64(time.Now().UnixNano())
	record.observe("random sequence seed %d", seed)
	stop, done := make(chan struct{}), make(chan []time.Time, 1)
	failed := make(chan error, 1)
	go func() {
		rng := rand.New(rand.NewPCG(seed, 0))
		state := maps.Clone(contents)
		var at []time.Time
		for n := 1; ; n++ {
			select {
			case <-stop:
				done <- at
				return
			default:
			}
			path := fmt.Sprintf("f%04d", rng.IntN(2400))
			old, exists := state[path]
			var err error
			if exists && rng.IntN(3) == 0 {
				err = developer.sendDelete(ctx, driveID, path, old)
				delete(state, path)
			} else {
				state[path] = fmt.Sprintf("v%d %s", n, path)
				err = developer.sendUpload(ctx, driveID, path, state[path])
			}
			if err != nil {
				failed <- err
				done <- at
				return
			}
			at = append(at, time.Now())
			time.Sleep(50 * time.Millisecond)
		}
	}()
	time.Sleep(time.Second)
	session, computers, rs := driveSession(t, e, "catch-up", []sessionDrive{readOnly(driveID)}, m)
	for deadline := time.Now().Add(3 * time.Minute); !listed.Load(); time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the mount's listing did not end within 3 minutes")
		}
	}
	time.Sleep(3 * time.Second)
	close(stop)
	commits := <-done
	select {
	case err := <-failed:
		t.Fatal(err)
	default:
	}
	pages := e.edge.answered(http.MethodGet, regexp.MustCompile(`^/v1/client/drives/`+driveID+`/files$`))
	during := 0
	for _, at := range commits {
		if len(pages) > 0 && at.After(pages[0].at) && at.Before(pages[len(pages)-1].ended) {
			during++
		}
	}
	if len(pages) < 2 || during == 0 {
		t.Fatalf("the mount listed %d pages with %d of %d commits made meanwhile; want several pages with commits between them", len(pages), during, len(commits))
	}
	record.observe("Session %s mounted the drive in %d list pages over %s; %d of the %d random commits were made meanwhile", session.ID, len(pages), pages[len(pages)-1].ended.Sub(pages[0].at).Round(time.Millisecond), during, len(commits))
	listing := rs.finished(rs.exec(computers[0], driveListing(driveID)), 10*time.Minute)
	if listing.Result.ExitCode == nil || *listing.Result.ExitCode != 0 {
		t.Fatalf("the listing command ended %+v; want exit 0", listing)
	}
	out := strings.TrimSpace(listing.Result.Stdout.Text)
	n := requireListingMatches(t, e, m, m.directory("catch-up"), driveID)
	record.observe("after the sequence, the mount lists %s files, the paths and contents of the %d files of drives.files.list", out, n)
}

// catchUpOutlivesRemovedVersionsAndExpiredCursors holds a machine's
// change requests while a file is written twelve times, which removes its
// oldest old version, and releases them: the catch-up succeeds without 410,
// lists the removed version's record and the command after it reads the
// current content. It then holds them again while a file is uploaded, ages
// every change record to 31 days and runs the daily drive cleanup: the
// developer route answers a cursor below them 410 drive_cursor_expired_error;
// once released, the client route answers 410 drive_cursor_expired, the CLI
// lists the drive again, keeps a change still pending and a cached file,
// whose download the edge then refuses, and its mount lists exactly the
// paths and contents of drives.files.list.
func catchUpOutlivesRemovedVersionsAndExpiredCursors(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	developer.upload(ctx, driveID, "cached.txt", "cached")
	m := onlineUserMachine(t, e, set, "machine")
	session, computers, rs := driveSession(t, e, "cursors", []sessionDrive{readWrite(driveID)}, m)
	if out, _ := rs.run(computers[0], "cat "+inDrive(driveID, "cached.txt"), time.Minute); out != "cached" {
		t.Fatalf("the first read of cached.txt printed %q", out)
	}
	changesRoute := regexp.MustCompile(`^/v1/client/drives/` + driveID + `/changes$`)
	filesRoute := regexp.MustCompile(`^/v1/client/drives/` + driveID + `/files$`)
	var holding atomic.Bool
	var mu sync.Mutex
	var answers []string
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool { return r.Method == http.MethodGet && changesRoute.MatchString(r.URL.Path) },
		before: func(_ http.ResponseWriter, r *http.Request) bool {
			for holding.Load() {
				select {
				case <-r.Context().Done():
					return true
				case <-time.After(50 * time.Millisecond):
				}
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
			mu.Lock()
			defer mu.Unlock()
			answers = append(answers, fmt.Sprintf("%d %s", response.StatusCode, body))
			return nil
		},
	})
	answered := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(answers)
	}

	holding.Store(true)
	oldest := developer.upload(ctx, driveID, "f", "v0")
	for i := 1; i <= 11; i++ {
		developer.upload(ctx, driveID, "f", fmt.Sprintf("v%d", i))
	}
	versions := developer.listVersions(ctx, driveID, "f")
	if slices.ContainsFunc(versions, func(v driveVersion) bool { return v.VersionID == oldest }) {
		t.Fatalf("f keeps its first version %s after 11 rewrites: %+v", oldest, versions)
	}
	record.observe("with the machine's change requests held, f was written 12 times; its first version %s is gone, %d versions remain", oldest, len(versions))
	before := len(answered())
	holding.Store(false)
	if out, _ := rs.run(computers[0], "cat "+inDrive(driveID, "f"), 2*time.Minute); out != "v11" {
		t.Fatalf("the command after the catch-up read f %q; want v11", out)
	}
	caught := answered()[before:]
	if len(caught) == 0 || !slices.ContainsFunc(caught, func(a string) bool { return strings.HasPrefix(a, "200 ") && strings.Contains(a, oldest) }) || slices.ContainsFunc(caught, func(a string) bool { return !strings.HasPrefix(a, "200 ") }) {
		t.Fatalf("the catch-up's change answers %q; want 200 answers only, one listing the record of %s", caught, oldest)
	}
	record.observe("released, the catch-up's %d change answers were 200, one listing the record of the removed version %s; the next command read v11", len(caught), oldest)

	if code, out := m.shell("sh", "-c", `printf pending > "$1/pending.txt"`, "sh", m.mountPoint(session.ID, driveID)); code != 0 {
		t.Fatalf("writing pending.txt outside the CLI exited %d: %s", code, out)
	}
	holding.Store(true)
	before = len(answered())
	developer.upload(ctx, driveID, "g", "g")
	newest := developer.listChanges(ctx, driveID, 0, "")
	gSeq := newest[len(newest)-1].Seq
	e.ownerTx(func(ctx context.Context, tx *dbconnect.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE drive_changes SET created_at=now()-interval '31 days' WHERE drive_id=$1 AND seq<=$2`, driveID, gSeq)
		return err
	})
	runCommand(t, builds.path(t, "tetral-cleanup"), e.cleanupEnv, nil, "drives")
	record.command("tetral-cleanup drives as the cleanup database role")
	status, _, answer := developer.do(ctx, http.MethodGet, fmt.Sprintf("/v1/drives/%s/changes?after=%d", driveID, gSeq-1), nil, "")
	if status != http.StatusGone || !strings.Contains(string(answer), `"drive_cursor_expired_error"`) {
		t.Fatalf("drives.changes.list after %d answered %d %s; want 410 drive_cursor_expired_error", gSeq-1, status, answer)
	}
	record.observe("every change record through seq %d aged 31 days and removed by the daily cleanup; drives.changes.list after %d answered 410 drive_cursor_expired_error", gSeq, gSeq-1)

	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool {
			return r.Method == http.MethodGet && r.URL.Path == "/v1/client/drives/"+driveID+"/content/"+sha256Hex("cached")
		},
		before: func(w http.ResponseWriter, _ *http.Request) bool {
			w.WriteHeader(http.StatusServiceUnavailable)
			return true
		},
	})
	expired := time.Now()
	holding.Store(false)
	for deadline := time.Now().Add(2 * time.Minute); ; time.Sleep(time.Second) {
		if slices.ContainsFunc(e.edge.answered(http.MethodGet, filesRoute), func(r edgeRequest) bool { return r.at.After(expired) && r.status == http.StatusOK }) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the CLI did not list the drive again within 2 minutes of the release")
		}
	}
	gone := answered()[before:]
	if len(gone) == 0 || !strings.HasPrefix(gone[0], "410 ") || !strings.Contains(gone[0], `"drive_cursor_expired"`) {
		t.Fatalf("the change answers after the cleanup %q; want 410 drive_cursor_expired first", gone)
	}
	out, envelope := rs.run(computers[0], driveListing(driveID), 5*time.Minute)
	if envelope.DriveSync == nil || envelope.DriveSync.Committed != 1 {
		t.Fatalf("the listing command's drive_sync %+v; want pending.txt committed", envelope.DriveSync)
	}
	n := requireListingMatches(t, e, m, m.directory("cursors"), driveID)
	record.observe("released, the client route answered 410 drive_cursor_expired and the CLI listed the drive again; with cached.txt's download refused, a command read all %s files, cached.txt from the cache, committed the pending pending.txt, and the mount lists the %d files of drives.files.list", out, n)
}
