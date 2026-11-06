package joint

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/protocol"
)

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

// TestOneCommandCommitsItsFilesTogether runs a command that writes two files
// into a drive. The CLI sends the two contents and then one commit; both
// change records carry that commit's seq, and the drive's other machine
// receives drive.changed with it.
func TestOneCommandCommitsItsFilesTogether(t *testing.T) {
	in := requireInputs(t, needs{})
	record := newAcceptanceRecord(t, in, platformOf(in))
	e := startEngine(t, record, engineOptions{})
	set := newMachines(t, in, needs{}, e, record)
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

// TestCommandsReadCommitsMadeBeforeTheirDispatch has Session S1 commit a
// file that Session S2 reads on another machine: S2's next command, dispatched
// after the commit, reads the new content, and again after S2's stream moved
// to another computer-hub pod.
func TestCommandsReadCommitsMadeBeforeTheirDispatch(t *testing.T) {
	in := requireInputs(t, needs{})
	record := newAcceptanceRecord(t, in, platformOf(in))
	e := startEngine(t, record, engineOptions{hubPods: 2})
	set := newMachines(t, in, needs{}, e, record)
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

// TestDownloadsRetryStallsAndFailOnMissingContent stalls the first download
// of a file for longer than 10 seconds after its first bytes: the CLI drops
// that attempt and tries again, and the read returns the whole file. A
// download answered 404 content_not_found fails its read at once.
func TestDownloadsRetryStallsAndFailOnMissingContent(t *testing.T) {
	e, set, record := scenario(t, engineOptions{})
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
