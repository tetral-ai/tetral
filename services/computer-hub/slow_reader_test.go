package computerhub

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/tetral-ai/tetral/integration/enginetest"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workload"
)

// slowBody hands an event stream's body to its reader at most rate bytes a
// second, in reads of at most 16 KiB; once it has handed stopAfter bytes,
// when that is positive, it hands nothing more until it is closed.
type slowBody struct {
	body      io.ReadCloser
	rate      int64
	stopAfter int64
	taken     atomic.Int64
	// last is the time of the last read that took bytes.
	last      atomic.Pointer[time.Time]
	stopped   chan struct{}
	stopOnce  sync.Once
	closed    chan struct{}
	closeOnce sync.Once
}

func (b *slowBody) Read(p []byte) (int, error) {
	if b.stopAfter > 0 && b.taken.Load() >= b.stopAfter {
		b.stopOnce.Do(func() { close(b.stopped) })
		<-b.closed
		return 0, io.ErrClosedPipe
	}
	n, err := b.body.Read(p[:min(len(p), 16<<10)])
	if n > 0 {
		now := time.Now()
		b.taken.Add(int64(n))
		b.last.Store(&now)
		time.Sleep(time.Duration(int64(n) * int64(time.Second) / b.rate))
	}
	return n, err
}

func (b *slowBody) Close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	return b.body.Close()
}

// slowTransport dials with a 4 KiB socket receive buffer and reads every
// event stream through a slowBody, which it hands to bodies.
type slowTransport struct {
	base      http.RoundTripper
	rate      int64
	stopAfter int64
	bodies    chan *slowBody
}

func (s slowTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := s.base.RoundTrip(request)
	if err != nil || request.URL.Path != protocol.ClientRoutePrefix+"events" {
		return response, err
	}
	body := &slowBody{body: response.Body, rate: s.rate, stopAfter: s.stopAfter, stopped: make(chan struct{}), closed: make(chan struct{})}
	response.Body = body
	s.bodies <- body
	return response, nil
}

func smallReceiveBuffer(_, _ string, raw syscall.RawConn) error {
	var setErr error
	err := raw.Control(func(fd uintptr) {
		setErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 4096)
	})
	return errors.Join(err, setErr)
}

// openSlow opens an online connection whose CLI reads its stream at rate
// bytes a second and, when stopAfter is positive, stops after that many.
func (f *hubFixture) openSlow(rate, stopAfter int64) (*enginetest.CLI, *testStream, *slowBody) {
	f.t.Helper()
	cli := f.userInstance(time.Time{})
	bodies := make(chan *slowBody, 1)
	dialer := &net.Dialer{Control: smallReceiveBuffer}
	cli.HTTP = &http.Client{Transport: slowTransport{base: &http.Transport{DialContext: dialer.DialContext}, rate: rate, stopAfter: stopAfter, bodies: bodies}}
	stream := f.open(cli)
	body := <-bodies
	f.acceptReady(cli, stream, "ready_1")
	return cli, stream, body
}

// issueBacklog issues edits tool.execute edits of size bytes of new content
// on connectionID in one transaction, so one pass writes them all. 16 MB in
// all is many times what the sockets between the pod and a stopped reader
// hold.
func (f *hubFixture) issueBacklog(connectionID string, edits, size int) []string {
	return f.issueBacklogContext(f.ctx, connectionID, edits, size)
}

func (f *hubFixture) issueBacklogContext(ctx context.Context, connectionID string, edits, size int) []string {
	f.t.Helper()
	payload := protocol.ExecutePayload{
		SessionID: "id_session_id", BindingID: "id_binding_id", ToolUseEventID: "id_tool_use_event_id", TaskID: "id_task_id",
		Input: protocol.EditInput{Path: "file", OldString: "before", NewString: protocol.Value(strings.Repeat("a", size))},
		SandboxPolicy: protocol.SandboxPolicy{
			Filesystem: protocol.FilesystemPolicy{Rules: []protocol.FilesystemRule{{Path: "/project", Access: "read_write"}}},
			Network:    protocol.NetworkPolicy{Mode: "disabled"},
		},
	}
	var messageIDs []string
	if err := f.client.WithWorkspaceTx(ctx, string(f.ws), "computer_hub.test_issue", func(tx *dbconnect.Tx) error {
		messageIDs = messageIDs[:0]
		connection, err := instance.ReadConnection(ctx, tx, f.ws, connectionID)
		if err != nil {
			return err
		}
		for range edits {
			messageID, err := instance.IssueInstruction(ctx, tx, f.ws, connection, payload, "")
			if err != nil {
				return err
			}
			messageIDs = append(messageIDs, messageID)
		}
		return nil
	}); err != nil {
		f.t.Fatal(err)
	}
	return messageIDs
}

// streamClosedAt waits until pod holds no stream and returns when it saw
// that.
func (f *hubFixture) streamClosedAt(pod *hubPod, within time.Duration) time.Time {
	f.t.Helper()
	deadline := time.Now().Add(within)
	for metricValue(f.t, pod.metrics, "computerhub_open_streams", "") != 0 {
		if time.Now().After(deadline) {
			f.t.Fatalf("the pod still holds its stream after %v", within)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return time.Now()
}

func TestStreamWritesWaitForSlowReadersButNotForStoppedOnes(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	a := f.pods[hubTestPod]
	b := f.addPod(secondPod)
	const rate = 1 << 20

	t.Run("a backlog read slowly for more than 10 seconds arrives whole on one stream", func(t *testing.T) {
		_, stream, _ := f.openSlow(rate, 0)
		started := time.Now()
		backlog := f.issueBacklog(stream.connectionID, 4, 4_000_000)
		f.requireInstructions(stream, backlog...)
		if read := time.Since(started); read <= 10*time.Second {
			t.Fatalf("the backlog was read in %v; want a read of more than 10 seconds", read)
		}
		if open := metricValue(t, a.metrics, "computerhub_open_streams", ""); open != 1 {
			t.Fatalf("open streams = %v after the backlog; want the stream kept", open)
		}
		_ = stream.stream.Close()
		f.streamClosedAt(a, 10*time.Second)
	})

	t.Run("a reader that stops loses its stream 10 seconds after its last byte and resumes", func(t *testing.T) {
		cli, stream, body := f.openSlow(rate, 6<<20)
		before := streamEnds(t, a.metrics, nil)
		backlog := f.issueBacklog(stream.connectionID, 4, 4_000_000)
		<-body.stopped
		last := *body.last.Load()
		// The pod's socket takes bytes in batches as the reader frees space,
		// so its last progress precedes the reader's last byte by up to the
		// time of one batch, under a second at this rate, and follows no
		// later than that read.
		if after := f.streamClosedAt(a, 20*time.Second).Sub(last); after < 8500*time.Millisecond || after > 10500*time.Millisecond {
			t.Fatalf("the stream closed %v after the reader's last byte; want 10 seconds", after)
		}
		waitUntil(t, func() bool { return len(streamEnds(t, a.metrics, before)) > 0 })
		if ends := streamEnds(t, a.metrics, before); len(ends) != 1 || ends["stalled"] != 1 {
			t.Fatalf("stream ends = %v; want the stream once as stalled", ends)
		}
		fast := *cli
		fast.HTTP = nil
		resumed := f.resume(b.url, &fast, stream.connectionID)
		if _, ok := resumed.next().Envelope.Payload.(protocol.HeartbeatPayload); !ok {
			t.Fatal("the resumed stream did not start with a heartbeat")
		}
		f.requireInstructions(resumed, backlog...)
		_ = resumed.stream.Close()
		f.streamClosedAt(b, 10*time.Second)
	})

	t.Run("a revocation commits during a write and closes the stream", func(t *testing.T) {
		cli, stream, body := f.openSlow(rate, 1<<20)
		f.issueBacklog(stream.connectionID, 4, 4_000_000)
		<-body.stopped
		revoking := time.Now()
		ctx, cancel := context.WithTimeout(f.ctx, 3*time.Second)
		defer cancel()
		request, err := http.NewRequestWithContext(ctx, http.MethodDelete, f.url+"/v1/instances/"+cli.InstanceID+"/credentials/"+cli.CredentialID, nil)
		if err != nil {
			t.Fatal(err)
		}
		token, err := f.signer.Mint(f.principal, request.Method, request.URL.Path, "req_hub", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("X-Tetral-Internal-Principal", token)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatalf("the revocation did not commit within 3 seconds of a write in progress: %v", err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusNoContent {
			t.Fatalf("revocation = %d", response.StatusCode)
		}
		if closed := f.streamClosedAt(a, 10*time.Second).Sub(revoking); closed > 3*time.Second {
			t.Fatalf("the stream closed %v after the revocation; want it closed with the revocation", closed)
		}
	})
}

func TestABacklogReadForMoreThan45SecondsKeepsItsConnection(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	a := f.pods[hubTestPod]
	cli, stream, _ := f.openSlow(320<<10, 0)
	started := time.Now()
	backlog := f.issueBacklog(stream.connectionID, 16, 1_000_000)
	// The reader takes about 49 seconds; the heartbeats due 15 and 30
	// seconds in fall due while the pod still writes the backlog.
	for _, due := range []time.Duration{15 * time.Second, 30 * time.Second} {
		time.AfterFunc(due, func() { a.clock.Advance(HeartbeatInterval) })
	}
	answered := 0
	for next := 0; next < len(backlog); {
		event := stream.next()
		if beat, ok := event.Envelope.Payload.(protocol.HeartbeatPayload); ok {
			response, err := cli.Heartbeat(f.ctx, protocol.Heartbeat{ConnectionID: stream.connectionID, MessageID: "msg_slow_hb_" + strconv.Itoa(answered), RequestMessageID: event.Envelope.MessageID, HeartbeatSeq: beat.HeartbeatSeq})
			if err != nil || response.Status != http.StatusOK {
				t.Fatalf("heartbeat answer = %d %s, %v", response.Status, response.Body, err)
			}
			answered++
			continue
		}
		if !bytes.Equal(event.Data, f.envelopeOf(backlog[next])) {
			t.Fatalf("event %d is not the backlog's next edit", next)
		}
		next++
	}
	if read := time.Since(started); read <= 45*time.Second {
		t.Fatalf("the backlog was read in %v; want a read of more than 45 seconds", read)
	}
	if answered != 2 {
		t.Fatalf("%d heartbeats answered during the backlog; want the 2 that fell due", answered)
	}
	if f.count(`SELECT count(*) FROM instance_connections WHERE id=$1 AND ended_at IS NULL AND lease_expires_at>clock_timestamp()`, stream.connectionID) != 1 {
		t.Fatal("the connection's lease passed during the backlog")
	}
}

// The reader reports the first instruction while the same pass is still
// writing its backlog. Reporting must not discard pending delivery recording.
func TestReportedInstructionStillRecordsItsSuccessfulWrite(t *testing.T) {
	spans := tracetest.NewInMemoryExporter()
	t.Cleanup(workload.InstallTelemetry(workload.NewTelemetry(workload.TelemetryOptions{Service: ServiceName, SyncExporter: spans, SampleRatio: 1})))
	f := newHubFixture(t, 720*time.Hour)
	_, stream, _ := f.openSlow(1<<20, 0)
	traced, span := workload.StartRequestSpan(f.ctx, "test.backlog", "")
	defer span.End()
	traceparent := workload.ContextTraceparent(traced)
	backlog := f.issueBacklogContext(traced, stream.connectionID, 3, 2_000_000)
	f.requireInstructions(stream, backlog[0])
	f.markReported(stream.connectionID, backlog[0])
	// The small socket buffer and unread four-megabyte tail hold this pass
	// in its network writes. Verify its recording pass has not committed.
	if n := f.count(`SELECT count(*) FROM connection_instructions WHERE connection_id=$1 AND first_written_at IS NOT NULL`, stream.connectionID); n != 0 {
		t.Fatalf("recording already advanced before report: %d rows", n)
	}
	if n := f.count(`SELECT count(*) FROM connection_instructions WHERE message_id=$1 AND reported_at IS NOT NULL`, backlog[0]); n != 1 {
		t.Fatal("report was not stored")
	}
	f.requireInstructions(stream, backlog[1:]...)
	sentinel := f.issueInstruction(traced, stream.connectionID, "sess_after_report")
	f.requireInstructions(stream, sentinel)
	awaitDeliverySpan(t, spans, sentinel)
	if n := f.count(`SELECT count(*) FROM connection_instructions WHERE message_id=$1 AND reported_at IS NOT NULL AND first_written_at IS NOT NULL`, backlog[0]); n != 1 {
		t.Fatal("reported instruction lost its pending first-write recording")
	}
	for _, messageID := range append(backlog, sentinel) {
		delivered := deliverySpans(spans, messageID)
		if len(delivered) != 1 || delivered[0].SpanContext.TraceID().String() != traceparent[3:35] {
			t.Fatalf("delivery %s: %v; want exactly one in issuing trace", messageID, delivered)
		}
	}
	// All four first marks are accounted for by message identity above;
	// the corresponding histogram must contain exactly four samples.
	if count := metricValue(t, f.pods[hubTestPod].metrics, "computerhub_instructions_queue_wait_seconds_count", ""); count != 4 {
		t.Fatalf("queue-wait count = %v; want 4", count)
	}
	requireNoEvent(t, stream)
}
