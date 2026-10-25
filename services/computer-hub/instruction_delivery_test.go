package computerhub

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/stdlib"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/tetral-ai/tetral/integration/enginetest"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workload"
)

const secondPod = "computer-hub-test-1"

// issueInstruction stores a session.close for sessionID on connectionID,
// under ctx's trace when it has one, and returns its message_id.
func (f *hubFixture) issueInstruction(ctx context.Context, connectionID, sessionID string) string {
	f.t.Helper()
	var messageID string
	if err := f.client.WithWorkspaceTx(ctx, string(f.ws), "computer_hub.test_issue", func(tx *dbconnect.Tx) error {
		connection, err := instance.ReadConnection(ctx, tx, f.ws, connectionID)
		if err != nil {
			return err
		}
		messageID, err = instance.IssueInstruction(ctx, tx, f.ws, connection, protocol.SessionClosePayload{SessionID: sessionID, BindingID: "bind_" + sessionID}, "")
		return err
	}); err != nil {
		f.t.Fatal(err)
	}
	return messageID
}

// storeWithoutNotice stores the next instruction of connectionID as
// IssueInstruction would, but sends no notice: a lost hint.
func (f *hubFixture) storeWithoutNotice(connectionID, sessionID string) string {
	f.t.Helper()
	var issueSeq int64
	if err := f.admin.QueryRowContext(f.ctx, `UPDATE instance_connections SET last_issue_seq=last_issue_seq+1 WHERE id=$1 RETURNING last_issue_seq`, connectionID).Scan(&issueSeq); err != nil {
		f.t.Fatal(err)
	}
	messageID := "msg_lost_" + strconv.FormatInt(issueSeq, 10)
	envelope := protocol.Encode(protocol.Downstream{ConnectionID: connectionID, MessageID: messageID, Payload: protocol.SessionClosePayload{SessionID: sessionID, BindingID: "bind_" + sessionID}})
	if _, err := f.admin.ExecContext(f.ctx, `INSERT INTO connection_instructions (workspace_id,connection_id,message_id,issue_seq,type,envelope) VALUES ($1,$2,$3,$4,'session.close',$5)`,
		string(f.ws), connectionID, messageID, issueSeq, envelope); err != nil {
		f.t.Fatal(err)
	}
	return messageID
}

func (f *hubFixture) markReported(connectionID, messageID string) {
	f.t.Helper()
	if err := f.client.WithWorkspaceTx(f.ctx, string(f.ws), "computer_hub.test_report", func(tx *dbconnect.Tx) error {
		return instance.MarkReported(f.ctx, tx, f.ws, connectionID, messageID)
	}); err != nil {
		f.t.Fatal(err)
	}
}

// envelopeOf reads the stored envelope of messageID.
func (f *hubFixture) envelopeOf(messageID string) []byte {
	f.t.Helper()
	var envelope []byte
	if err := f.admin.QueryRowContext(f.ctx, `SELECT envelope FROM connection_instructions WHERE message_id=$1`, messageID).Scan(&envelope); err != nil {
		f.t.Fatal(err)
	}
	return envelope
}

// requireInstructions reads the stream's next events, which must be the
// stored envelopes of messageIDs, byte for byte and in order.
func (f *hubFixture) requireInstructions(stream *testStream, messageIDs ...string) {
	f.t.Helper()
	for _, messageID := range messageIDs {
		event := stream.next()
		if !bytes.Equal(event.Data, f.envelopeOf(messageID)) {
			f.t.Fatalf("event %s; want instruction %s as stored", event.Data, messageID)
		}
	}
}

// dbNow is the database clock.
func (f *hubFixture) dbNow() time.Time {
	f.t.Helper()
	var now time.Time
	if err := f.admin.QueryRowContext(f.ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		f.t.Fatal(err)
	}
	return now
}

// openOnline opens a new connection for a new user instance on pod A and
// accepts its ready.
func (f *hubFixture) openOnline() (*enginetest.CLI, *testStream) {
	f.t.Helper()
	cli := f.userInstance(time.Time{})
	stream := f.open(cli)
	f.acceptReady(cli, stream, "ready_1")
	return cli, stream
}

func TestResumeOnAnotherPodResendsEachUnreportedInstructionOnce(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	b := f.addPod(secondPod)
	cli, stream := f.openOnline()
	first := f.issueInstruction(f.ctx, stream.connectionID, "sess_first")
	second := f.issueInstruction(f.ctx, stream.connectionID, "sess_second")
	answered := f.issueInstruction(f.ctx, stream.connectionID, "sess_answered")
	f.requireInstructions(stream, first, second, answered)
	f.markReported(stream.connectionID, answered)
	before := f.connectionRow(stream.connectionID)

	resumed := f.resume(b.url, cli, stream.connectionID)
	if _, ok := resumed.next().Envelope.Payload.(protocol.HeartbeatPayload); !ok {
		t.Fatal("the resumed stream did not start with a heartbeat")
	}
	f.requireInstructions(resumed, first, second)
	requireNoEvent(t, resumed)
	if events := stream.ended(); len(events) != 0 {
		t.Fatalf("pod A's stream wrote %s after the resume; want it closed without a disconnect", events[0].Data)
	}
	after := f.connectionRow(stream.connectionID)
	if after["lease_expires_at"] != before["lease_expires_at"] || after["hub_pod"] != secondPod || after["stream_count"] != float64(2) {
		t.Fatalf("connection after the resume = %v; want the same lease, hub_pod %s and stream_count 2", after, secondPod)
	}
}

func TestLateResumeHeartbeatRenewsBeyondTheEarlierGrant(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	b := f.addPod(secondPod)
	cli, stream := f.openOnline()
	// The lease as a grant 40 seconds ago left it: 5 seconds remain.
	if _, err := f.admin.ExecContext(f.ctx, `UPDATE instance_connections SET lease_expires_at=clock_timestamp()+interval '5 seconds' WHERE id=$1`, stream.connectionID); err != nil {
		t.Fatal(err)
	}
	earlier := parseTime(t, f.connectionRow(stream.connectionID)["lease_expires_at"])
	resumed := f.resume(b.url, cli, stream.connectionID)
	report := resumed.heartbeat("heartbeat_after_resume")
	sent := f.dbNow()
	// Reports reach any pod: the answer goes to pod A.
	response, err := cli.Heartbeat(f.ctx, report)
	if err != nil || response.Status != http.StatusOK {
		t.Fatalf("heartbeat = %d %s, %v", response.Status, response.Body, err)
	}
	answered := f.dbNow()
	lease := parseTime(t, f.connectionRow(stream.connectionID)["lease_expires_at"])
	if lease.Before(sent.Add(instance.LeaseGrant)) || lease.After(answered.Add(instance.LeaseGrant)) || !lease.After(earlier.Add(30*time.Second)) {
		t.Fatalf("lease %v after the answer between %v and %v; want 45 seconds after it, past the earlier grant %v", lease, sent, answered, earlier)
	}
}

func TestStartsWithoutTheLiveConnectionReplaceNothing(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	cli, stream := f.openOnline()
	resume := true
	for name, query := range map[string]map[string][]string{
		"resume of another connection":   cli.EventsQuery("conn_"+strings.Repeat("0", 32), &resume),
		"fresh start without a previous": cli.EventsQuery("", nil),
	} {
		before := f.connectionRow(stream.connectionID)
		opened, response, err := cli.OpenEvents(f.ctx, query)
		if opened != nil {
			t.Fatalf("%s opened a stream", name)
		}
		requireClientError(t, response, err, protocol.ErrorConnectionConflict)
		requireRowUnchanged(t, before, f.connectionRow(stream.connectionID))
		if n := f.count(`SELECT count(*) FROM instance_connections WHERE instance_id=$1`, cli.InstanceID); n != 1 {
			t.Fatalf("%s: connections = %d; want the live one only", name, n)
		}
	}
}

func TestResumeFalseWaitsForTheRemainingLease(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	cli, stream := f.openOnline()
	before := f.connectionRow(stream.connectionID)
	lease := parseTime(t, before["lease_expires_at"])
	resume := false
	sent := f.dbNow()
	opened, response, err := cli.OpenEvents(f.ctx, cli.EventsQuery(stream.connectionID, &resume))
	answered := f.dbNow()
	if opened != nil || err != nil {
		t.Fatalf("resume=false opened a stream, %v", err)
	}
	body, decodeErr := response.Error()
	ceilMS := func(d time.Duration) int64 { return int64((d + time.Millisecond - 1) / time.Millisecond) }
	if decodeErr != nil || response.Status != http.StatusConflict || body.Code != protocol.ErrorPreviousConnectionActive || !body.RetryAfterMS.Set ||
		body.RetryAfterMS.Value < ceilMS(lease.Sub(answered)) || body.RetryAfterMS.Value > ceilMS(lease.Sub(sent)) {
		t.Fatalf("answer = %d %s; want previous_connection_active with retry_after_ms the lease left between %v and %v", response.Status, response.Body, sent, answered)
	}
	requireRowUnchanged(t, before, f.connectionRow(stream.connectionID))

	// Once the lease has passed, the same request starts a new connection.
	if _, err := f.admin.ExecContext(f.ctx, `UPDATE instance_connections SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, stream.connectionID); err != nil {
		t.Fatal(err)
	}
	fresh := f.openClientStream(f.url, cli, cli.EventsQuery(stream.connectionID, &resume), false)
	if fresh.connectionID == stream.connectionID || f.connectionRow(stream.connectionID)["end_cause"] != instance.EndLeaseExpired {
		t.Fatalf("connection after the lease = %s; want a new one and the old ended lease_expired", fresh.connectionID)
	}
}

func TestStreamEndsAtItsMaximumAgeAndTheConnectionResumes(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour, withStreamMaxAge(10*time.Minute))
	cli := f.userInstance(time.Time{})
	stream := f.open(cli)
	f.clock.waitTimers(t, 1)
	f.clock.Advance(10*time.Minute - time.Millisecond)
	requireNoEvent(t, stream)
	f.clock.Advance(time.Minute + time.Millisecond)
	events := stream.ended()
	if len(events) != 1 || events[0].Envelope.Payload != (protocol.DisconnectPayload{Scope: "stream"}) {
		t.Fatalf("events by 11 minutes = %d; want one stream-only disconnect", len(events))
	}
	f.resume(f.url, cli, stream.connectionID)
}

func TestNoticesReachOnlyThePodHoldingTheStream(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	b := f.addPod(secondPod)
	cli, stream := f.openOnline()
	notices := f.listen(instance.ConnectionNoticeChannel(hubTestPod), instance.ConnectionNoticeChannel(secondPod))
	first := f.issueInstruction(f.ctx, stream.connectionID, "sess_first")
	f.requireInstructions(stream, first)
	got := notices.within(300 * time.Millisecond)
	want := instance.ConnectionNotice{WorkspaceID: string(f.ws), ConnectionID: stream.connectionID, IssueSeq: 1}
	if len(got) != 1 || got[0].channel != instance.ConnectionNoticeChannel(hubTestPod) || got[0].notice != want {
		t.Fatalf("notices = %+v; want one %+v on pod A's channel only", got, want)
	}

	lost := f.storeWithoutNotice(stream.connectionID, "sess_lost")
	requireNoEvent(t, stream)
	f.restartListener(hubTestPod)
	f.requireInstructions(stream, lost)

	resumed := f.resume(b.url, cli, stream.connectionID)
	resumed.next()
	f.requireInstructions(resumed, first, lost)
}

func TestSixtyFourInstructionsGoOutBeforeTheFirstSessionReport(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	_, stream := f.openOnline()
	var issued []string
	for i := range 70 {
		issued = append(issued, f.issueInstruction(f.ctx, stream.connectionID, "sess_"+strconv.Itoa(i)))
	}
	f.requireInstructions(stream, issued[:instance.SessionInstructionCap]...)
	requireNoEvent(t, stream)
	if err := f.client.WithWorkspaceTx(f.ctx, string(f.ws), "computer_hub.test_report", func(tx *dbconnect.Tx) error {
		return instance.MarkFirstSessionReport(f.ctx, tx, f.ws, stream.connectionID)
	}); err != nil {
		t.Fatal(err)
	}
	f.requireInstructions(stream, issued[instance.SessionInstructionCap:]...)
	requireNoEvent(t, stream)
}

func TestRevocationAndInstructionWritesNeverInterleave(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	revoke := func(cli *enginetest.CLI) chan int {
		done := make(chan int, 1)
		go func() {
			status, _ := f.do(http.MethodDelete, "/v1/instances/"+cli.InstanceID+"/credentials/"+cli.CredentialID, nil, "")
			done <- status
		}()
		return done
	}
	requireDisconnect := func(event enginetest.Event) {
		t.Helper()
		if event.Envelope.Payload != (protocol.DisconnectPayload{Scope: "connection", Retryable: protocol.Some(false), Code: protocol.Some(string(protocol.ErrorCredentialRevoked))}) {
			t.Fatalf("event %s; want the revocation's disconnect", event.Data)
		}
	}

	t.Run("a write holding the row commits first", func(t *testing.T) {
		cli, stream := f.openOnline()
		// A first instruction, written and committed, shows that no earlier
		// pass is still running when the pause is set.
		warmup := f.issueInstruction(f.ctx, stream.connectionID, "sess_warmup")
		f.requireInstructions(stream, warmup)
		waitUntil(t, func() bool {
			return f.count(`SELECT count(*) FROM connection_instructions WHERE message_id=$1 AND first_written_at IS NOT NULL`, warmup) == 1
		})
		pause := f.pauseOn("reported_at IS NULL AND issue_seq<=")
		instruction := f.issueInstruction(f.ctx, stream.connectionID, "sess_racing")
		<-pause.reached
		revoked := revoke(cli)
		select {
		case <-revoked:
			t.Fatal("the revocation committed while a write held the connection row")
		case <-time.After(300 * time.Millisecond):
		}
		close(pause.release)
		events := stream.ended()
		if len(events) != 2 || !bytes.Equal(events[0].Data, f.envelopeOf(instruction)) {
			t.Fatalf("events = %d; want the instruction, then the disconnect", len(events))
		}
		requireDisconnect(events[1])
		if status := <-revoked; status != http.StatusNoContent {
			t.Fatalf("revocation = %d", status)
		}
	})

	t.Run("a revocation holding the row commits first", func(t *testing.T) {
		cli, stream := f.openOnline()
		lost := f.storeWithoutNotice(stream.connectionID, "sess_late")
		pause := f.pauseOn("DELETE FROM connection_upstream_messages")
		revoked := revoke(cli)
		<-pause.reached
		notice, _ := json.Marshal(instance.ConnectionNotice{WorkspaceID: string(f.ws), ConnectionID: stream.connectionID, IssueSeq: 1})
		if _, err := f.admin.ExecContext(f.ctx, `SELECT pg_notify($1,$2)`, instance.ConnectionNoticeChannel(hubTestPod), string(notice)); err != nil {
			t.Fatal(err)
		}
		requireNoEvent(t, stream)
		close(pause.release)
		events := stream.ended()
		if len(events) != 1 {
			t.Fatalf("events = %d; want only the disconnect, never %s", len(events), lost)
		}
		requireDisconnect(events[0])
		<-revoked
	})
}

// TestWritersCommitWhileAPassTakesItsLocks holds a write pass at its first
// lock and shows that a revocation, a deregistration and an activation each
// commit meanwhile, so none of them waits on the pass in a cycle.
func TestWritersCommitWhileAPassTakesItsLocks(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	b := f.addPod(secondPod)
	request := func(method, path string) func() <-chan int {
		return func() <-chan int {
			done := make(chan int, 1)
			go func() {
				status, _ := f.do(method, path, nil, "")
				done <- status
			}()
			return done
		}
	}
	disconnect := func(code protocol.ErrorCode) []protocol.DownstreamPayload {
		return []protocol.DownstreamPayload{protocol.DisconnectPayload{Scope: "connection", Retryable: protocol.Some(false), Code: protocol.Some(string(code))}}
	}
	for _, row := range []struct {
		name string
		// prepare readies the writer and returns what starts it.
		prepare func(cli *enginetest.CLI, stream *testStream) func() <-chan int
		// events is what the held pass's stream writes before it ends.
		events []protocol.DownstreamPayload
	}{
		{"revocation", func(cli *enginetest.CLI, _ *testStream) func() <-chan int {
			return request(http.MethodDelete, "/v1/instances/"+cli.InstanceID+"/credentials/"+cli.CredentialID)
		}, disconnect(protocol.ErrorCredentialRevoked)},
		{"deregistration", func(cli *enginetest.CLI, _ *testStream) func() <-chan int {
			return request(http.MethodDelete, "/v1/instances/"+cli.InstanceID)
		}, disconnect(protocol.ErrorInstanceUnavailable)},
		{"activation", func(cli *enginetest.CLI, stream *testStream) func() <-chan int {
			pending := f.renewal(cli, stream, "renew-during-pass", `{}`)["id"].(string)
			renewed := f.fetched(cli, stream.connectionID, pending)
			renewed.BaseURL = b.url
			resume := true
			query := renewed.EventsQuery(stream.connectionID, &resume)
			return func() <-chan int {
				done := make(chan int, 1)
				go func() {
					opened, response, err := renewed.OpenEvents(f.ctx, query)
					if opened != nil {
						t.Cleanup(func() { _ = opened.Close() })
					}
					if err != nil {
						done <- 0
						return
					}
					done <- response.Status
				}()
				return done
			}
		}, nil},
	} {
		t.Run(row.name, func(t *testing.T) {
			cli, stream := f.openOnline()
			start := row.prepare(cli, stream)
			pause := f.pauseOn("FROM instances WHERE workspace_id=$1 AND id=$2 FOR SHARE")
			release := sync.OnceFunc(func() { close(pause.release) })
			defer release()
			issued, cancel := context.WithTimeout(f.ctx, 10*time.Second)
			defer cancel()
			f.issueInstruction(issued, stream.connectionID, "sess_"+row.name)
			<-pause.reached
			answered := start()
			select {
			case status := <-answered:
				release()
				if status != http.StatusNoContent && status != http.StatusOK {
					t.Fatalf("%s = %d while the pass was held", row.name, status)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("the %s did not commit while the pass was held at its first lock", row.name)
			}
			var payloads []protocol.DownstreamPayload
			for _, event := range stream.ended() {
				payloads = append(payloads, event.Envelope.Payload)
			}
			if fmt.Sprint(payloads) != fmt.Sprint(row.events) {
				t.Fatalf("the held pass's stream wrote %v; want %v", payloads, row.events)
			}
		})
	}
}

// TestFailedPassesWaitLongerUntilAPassCommits fails a stream's passes one
// after another: each retry waits twice the previous wait, at most
// passRetryMax, times 1+r with r in 0 to 0.2, and after a pass commits the
// next failure waits passRetryFirst again.
func TestFailedPassesWaitLongerUntilAPassCommits(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	// A pass's instance lock, which only passes take in this test.
	const passLock = "FROM instances WHERE workspace_id=$1 AND id=$2 FOR SHARE"
	_, stream := f.openOnline()
	// retries returns the waits the stream asked its clock for after failed
	// passes: every request but the age limit and the heartbeats.
	seen := 2
	nextRetry := func(base time.Duration) time.Duration {
		t.Helper()
		for {
			seen++
			if wait := f.clock.waitRequest(t, seen); wait != HeartbeatInterval {
				if wait < base || wait > base+base/5 {
					t.Fatalf("retry waits %v; want %v to %v", wait, base, base+base/5)
				}
				return wait
			}
		}
	}
	f.clock.waitTimers(t, 2)
	f.failOn(passLock)
	instruction := f.issueInstruction(f.ctx, stream.connectionID, "sess_retried")
	base := passRetryFirst
	for range 6 {
		wait := nextRetry(base)
		f.failOn(passLock)
		f.clock.Advance(wait)
		base = min(2*base, passRetryMax)
	}
	f.clock.Advance(nextRetry(base))
	for event := stream.next(); !bytes.Equal(event.Data, f.envelopeOf(instruction)); event = stream.next() {
		if _, ok := event.Envelope.Payload.(protocol.HeartbeatPayload); !ok {
			t.Fatalf("event %s; want heartbeats, then the instruction", event.Data)
		}
	}
	f.failOn(passLock)
	notice, _ := json.Marshal(instance.ConnectionNotice{WorkspaceID: string(f.ws), ConnectionID: stream.connectionID})
	if _, err := f.admin.ExecContext(f.ctx, `SELECT pg_notify($1,$2)`, instance.ConnectionNoticeChannel(hubTestPod), string(notice)); err != nil {
		t.Fatal(err)
	}
	nextRetry(passRetryFirst)
}

// TestFirstWriteOfAFailedPassIsRecordedWithoutAResend fails a pass after it
// wrote an instruction: the stream's next pass records that first write and
// does not write the instruction again.
func TestFirstWriteOfAFailedPassIsRecordedWithoutAResend(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	_, stream := f.openOnline()
	f.clock.waitTimers(t, 2)
	f.failOn("SET first_written_at")
	instruction := f.issueInstruction(f.ctx, stream.connectionID, "sess_failed_pass")
	f.requireInstructions(stream, instruction)
	f.clock.waitTimers(t, 3)
	if f.count(`SELECT count(*) FROM connection_instructions WHERE message_id=$1 AND first_written_at IS NULL`, instruction) != 1 {
		t.Fatal("the failed pass recorded the instruction's first write")
	}
	f.clock.Advance(2 * time.Second)
	waitUntil(t, func() bool {
		return f.count(`SELECT count(*) FROM connection_instructions WHERE message_id=$1 AND first_written_at IS NOT NULL`, instruction) == 1
	})
	requireNoEvent(t, stream)
}

func TestResentInstructionKeepsItsTraceAndMakesOneDeliverSpan(t *testing.T) {
	spans := tracetest.NewInMemoryExporter()
	t.Cleanup(workload.InstallTelemetry(workload.NewTelemetry(workload.TelemetryOptions{Service: ServiceName, SyncExporter: spans, SampleRatio: 1})))
	f := newHubFixture(t, 720*time.Hour)
	b := f.addPod(secondPod)
	cli, stream := f.openOnline()
	traced, span := workload.StartRequestSpan(f.ctx, "test.issue", "")
	traceparent := workload.ContextTraceparent(traced)
	instruction := f.issueInstruction(traced, stream.connectionID, "sess_traced")
	span.End()

	if event := stream.next(); event.Envelope.Traceparent != protocol.Some(traceparent) {
		t.Fatalf("first write %s; want traceparent %s", event.Data, traceparent)
	}
	resumed := f.resume(b.url, cli, stream.connectionID)
	resumed.next()
	if event := resumed.next(); event.Envelope.Traceparent != protocol.Some(traceparent) || !bytes.Equal(event.Data, f.envelopeOf(instruction)) {
		t.Fatalf("resend %s; want the stored envelope with traceparent %s", event.Data, traceparent)
	}
	var delivers []tracetest.SpanStub
	for _, stub := range spans.GetSpans() {
		if stub.Name == "instruction.deliver" {
			delivers = append(delivers, stub)
		}
	}
	if len(delivers) != 1 || delivers[0].SpanContext.TraceID().String() != traceparent[3:35] {
		t.Fatalf("instruction.deliver spans = %d; want one in the issuing trace", len(delivers))
	}
	attributes := map[string]string{}
	for _, attribute := range delivers[0].Attributes {
		attributes[string(attribute.Key)] = attribute.Value.String()
	}
	if attributes["message.id"] != instruction || attributes["connection.id"] != stream.connectionID || attributes["message.type"] != "session.close" {
		t.Fatalf("span attributes = %v", attributes)
	}
	if started := metricValue(t, f.pods[hubTestPod].metrics, "computerhub_connections_started_total", "new") + metricValue(t, b.metrics, "computerhub_connections_started_total", "new"); started != 1 {
		t.Fatalf("new connections counted = %v; want 1", started)
	}
	if resumes := metricValue(t, b.metrics, "computerhub_connections_started_total", "resumed"); resumes != 1 {
		t.Fatalf("resumes counted = %v; want 1", resumes)
	}
}

func TestInstructionWithoutAStreamCountsAsQueued(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	_, stream := f.openOnline()
	written := f.issueInstruction(f.ctx, stream.connectionID, "sess_written")
	f.requireInstructions(stream, written)
	waitUntil(t, func() bool {
		return f.count(`SELECT count(*) FROM connection_instructions WHERE message_id=$1 AND first_written_at IS NOT NULL`, written) == 1
	})
	_ = stream.stream.Close()
	waitUntil(t, func() bool { return f.streams.lookup(stream.connectionID) == nil })
	f.issueInstruction(f.ctx, stream.connectionID, "sess_waiting")
	now := f.dbNow()
	collect := instructionPressure(f.client, func() time.Time { return now })
	read := func() map[string]float64 {
		t.Helper()
		metrics, err := collect(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		values := map[string]float64{}
		for _, metric := range metrics {
			values[metric.Name] = metric.Value
		}
		return values
	}
	first := read()
	now = now.Add(90 * time.Second)
	later := read()
	if first["computerhub_instructions_queued"] != 1 || first["computerhub_instructions_in_progress"] != 1 ||
		later["computerhub_instructions_oldest_queued_seconds"]-first["computerhub_instructions_oldest_queued_seconds"] != 90 {
		t.Fatalf("pressure = %v then %v; want one written, one queued whose age grows by 90 seconds", first, later)
	}
}

// metricValue is the value of family's series whose only label is value,
// or of its unlabelled series when value is empty.
func metricValue(t *testing.T, metrics *hubMetrics, family, value string) float64 {
	t.Helper()
	samples, err := metrics.registry.Collector()(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, sample := range samples {
		if sample.Name == family && (value == "" && len(sample.Labels) == 0 || len(sample.Labels) == 1 && sample.Labels[0].Value == value) {
			return sample.Value
		}
	}
	return 0
}

func waitUntil(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 10 seconds")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// restartListener ends the LISTEN connection of pod's notice channel, which
// the pod then opens again.
func (f *hubFixture) restartListener(pod string) {
	f.t.Helper()
	listen := `LISTEN ` + instance.ConnectionNoticeChannel(pod)
	waitUntil(f.t, func() bool {
		return f.count(`SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND query=$1`, listen) == 1
	})
	if _, err := f.admin.ExecContext(f.ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=current_database() AND query=$1`, listen); err != nil {
		f.t.Fatal(err)
	}
}

type receivedNotice struct {
	channel string
	notice  instance.ConnectionNotice
}

// noticeListener receives the notices of some channels on a connection of
// its own.
type noticeListener struct {
	t    *testing.T
	conn *sql.Conn
}

func (f *hubFixture) listen(channels ...string) *noticeListener {
	f.t.Helper()
	conn, err := f.admin.Conn(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { _ = conn.Close() })
	for _, channel := range channels {
		if _, err := conn.ExecContext(f.ctx, `LISTEN "`+channel+`"`); err != nil {
			f.t.Fatal(err)
		}
	}
	return &noticeListener{t: f.t, conn: conn}
}

// within returns every notice that arrives within wait.
func (l *noticeListener) within(wait time.Duration) []receivedNotice {
	l.t.Helper()
	var received []receivedNotice
	deadline := time.Now().Add(wait)
	err := l.conn.Raw(func(driverConn any) error {
		pgxConn := driverConn.(*stdlib.Conn).Conn()
		for {
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			notification, err := pgxConn.WaitForNotification(ctx)
			cancel()
			if err != nil {
				return nil
			}
			var notice instance.ConnectionNotice
			if err := json.Unmarshal([]byte(notification.Payload), &notice); err != nil {
				return err
			}
			received = append(received, receivedNotice{channel: notification.Channel, notice: notice})
		}
	})
	if err != nil {
		l.t.Fatal(err)
	}
	slices.SortFunc(received, func(a, b receivedNotice) int { return strings.Compare(a.channel, b.channel) })
	return received
}
