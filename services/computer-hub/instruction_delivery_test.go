package computerhub

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
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
	if lease.Before(sent.Add(45*time.Second)) || lease.After(answered.Add(45*time.Second)) || !lease.After(earlier.Add(30*time.Second)) {
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

func TestRetryAfterIsTheLeaseLeftInMillisecondsRoundedUp(t *testing.T) {
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		left time.Duration
		want int64
	}{
		{time.Nanosecond, 1},
		{time.Millisecond, 1},
		{time.Millisecond + 100*time.Nanosecond, 2},
		{44_999*time.Millisecond + 500*time.Microsecond, 45_000},
	} {
		if got := retryAfterMS(now.Add(tc.left), now); got != tc.want {
			t.Errorf("retry_after_ms with %v left = %d; want %d", tc.left, got, tc.want)
		}
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

// TestEveryStreamOfAStoppingPodGetsItsDisconnect drains a pod with a window
// of 0 and stops its streams at once, as Run does once the drain is due,
// while each stream waits in a pass for a row another transaction holds, so
// that it sees its drain and its close together once the row is free: each
// stream still writes its stream-only disconnect before it closes.
func TestEveryStreamOfAStoppingPodGetsItsDisconnect(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	const count = 12
	var streams []*testStream
	holder, err := f.admin.BeginTx(f.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback() }()
	for range count {
		cli, stream := f.openOnline()
		streams = append(streams, stream)
		if _, err := holder.ExecContext(f.ctx, `SELECT 1 FROM instances WHERE id=$1 FOR NO KEY UPDATE`, cli.InstanceID); err != nil {
			t.Fatal(err)
		}
	}
	for _, stream := range streams {
		notice, _ := json.Marshal(instance.ConnectionNotice{WorkspaceID: string(f.ws), ConnectionID: stream.connectionID})
		if _, err := f.admin.ExecContext(f.ctx, `SELECT pg_notify($1,$2)`, instance.ConnectionNoticeChannel(hubTestPod), string(notice)); err != nil {
			t.Fatal(err)
		}
	}
	waitUntil(t, func() bool {
		return f.count(`SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'`) == count
	})
	f.streams.Drain(f.ctx, 0)
	f.streams.closeAll()
	if err := holder.Rollback(); err != nil {
		t.Fatal(err)
	}
	for i, stream := range streams {
		events := stream.ended()
		if len(events) != 1 || events[0].Envelope.Payload != (protocol.DisconnectPayload{Scope: "stream"}) {
			t.Fatalf("stream %d wrote %d events before it closed; want only the stream-only disconnect", i, len(events))
		}
	}
	metrics := f.pods[hubTestPod].metrics
	waitUntil(t, func() bool { return streamEnds(t, metrics, nil)["drain"] == count })
	if ends := streamEnds(t, metrics, nil); len(ends) != 1 {
		t.Fatalf("stream ends = %v; want each of the %d streams once as drain", ends, count)
	}
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

// TestStreamsHeldBeforeTheFirstListenAreWokenOnceItIsInPlace holds a stream
// on a pod whose listener has not yet listened, as a request served between
// readiness and the first LISTEN does: once the LISTEN is in place the
// stream is woken to write everything its connection holds.
func TestStreamsHeldBeforeTheFirstListenAreWokenOnceItIsInPlace(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	streams := NewStreams(f.client, "computer-hub-test-early", DefaultStreamMaxAge, newManualClock(), f.logger, newHubMetrics())
	held := streams.hold(f.ws, "conn_early", 1)
	// The stream's first pass takes the release its start gives.
	held.take()
	go func() { _ = streams.Run(f.ctx) }()
	select {
	case <-streams.listening:
	case <-time.After(10 * time.Second):
		t.Fatal("the pod did not listen within 10 seconds")
	}
	select {
	case <-held.wake:
	default:
		t.Fatal("the first LISTEN woke no held stream")
	}
	if _, all := held.take(); !all {
		t.Fatal("the first LISTEN did not release every instruction of the held stream")
	}
}

func TestSixtyFourInstructionsGoOutBeforeTheFirstSessionReport(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	_, stream := f.openOnline()
	var issued []string
	for i := range 70 {
		issued = append(issued, f.issueInstruction(f.ctx, stream.connectionID, "sess_"+strconv.Itoa(i)))
	}
	f.requireInstructions(stream, issued[:64]...)
	requireNoEvent(t, stream)
	if err := f.client.WithWorkspaceTx(f.ctx, string(f.ws), "computer_hub.test_report", func(tx *dbconnect.Tx) error {
		return instance.MarkFirstSessionReport(f.ctx, tx, f.ws, stream.connectionID)
	}); err != nil {
		t.Fatal(err)
	}
	f.requireInstructions(stream, issued[64:]...)
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

	t.Run("a pass holding the row chooses before the revocation commits", func(t *testing.T) {
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
			t.Fatal("the revocation committed while a pass held the connection row")
		case <-time.After(300 * time.Millisecond):
		}
		close(pause.release)
		// The pass writes its choice after it commits, and the revocation's
		// notice may cut that write: the stream ends with at most the
		// instruction, then the disconnect.
		events := stream.ended()
		if len(events) > 2 || len(events) > 0 && !bytes.Equal(events[0].Data, f.envelopeOf(instruction)) {
			t.Fatalf("events = %d; want at most the instruction, then the disconnect", len(events))
		}
		if len(events) == 2 {
			requireDisconnect(events[1])
		}
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
// after another: the first retry waits passRetryFirst, each later one twice
// the previous wait, at most passRetryMax, times 1+r with r in 0 to 0.2, and
// after a pass commits the next failure waits passRetryFirst again.
func TestFailedPassesWaitLongerUntilAPassCommits(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	// A pass's instance lock, which only passes take in this test.
	const passLock = "FROM instances WHERE workspace_id=$1 AND id=$2 FOR SHARE"
	_, stream := f.openOnline()
	// retries returns the waits the stream asked its clock for after failed
	// passes: every request but the age limit and the heartbeats.
	seen, jittered := 2, false
	nextRetry := func(base time.Duration) time.Duration {
		t.Helper()
		for {
			seen++
			if wait := f.clock.waitRequest(t, seen); wait != HeartbeatInterval {
				if wait < base || wait > base+base/5 {
					t.Fatalf("retry waits %v; want %v to %v", wait, base, base+base/5)
				}
				jittered = jittered || wait > base
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
	if !jittered {
		t.Fatal("no retry wait was above its base; want each times 1+r")
	}
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

// TestAPassHeldPastItsDeadlineFailsAndRetries holds an online instance's row
// in another transaction while an instruction is issued: the pass gives up
// passDeadline after it began, while the row is still held, asks to retry
// after the first wait, and delivers the instruction once the row is free.
func TestAPassHeldPastItsDeadlineFailsAndRetries(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	cli, stream := f.openOnline()
	f.clock.waitTimers(t, 2)
	holder, err := f.admin.BeginTx(f.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback() }()
	// FOR NO KEY UPDATE holds back the pass's FOR SHARE, not the issuing
	// transaction's foreign key checks.
	if _, err := holder.ExecContext(f.ctx, `SELECT 1 FROM instances WHERE id=$1 FOR NO KEY UPDATE`, cli.InstanceID); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	instruction := f.issueInstruction(f.ctx, stream.connectionID, "sess_held")
	// The first wait the stream asks for after the age limit and the
	// heartbeat.
	retry := func() (time.Duration, bool) {
		f.clock.mu.Lock()
		defer f.clock.mu.Unlock()
		for _, wait := range f.clock.requested[2:] {
			if wait != HeartbeatInterval {
				return wait, true
			}
		}
		return 0, false
	}
	wait, asked := retry()
	for ; !asked; wait, asked = retry() {
		if time.Since(started) > passDeadline+10*time.Second {
			t.Fatalf("the held pass asked for no retry within %v", passDeadline+10*time.Second)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if elapsed := time.Since(started); elapsed < passDeadline || elapsed > passDeadline+3*time.Second || wait < passRetryFirst || wait > passRetryFirst+passRetryFirst/5 {
		t.Fatalf("retry of %v asked %v after the issue; want the first wait, asked at the pass deadline %v", wait, elapsed, passDeadline)
	}
	if err := holder.Rollback(); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(wait)
	f.requireInstructions(stream, instruction)
}

// TestFirstWriteIsRecordedWithoutAResendWhenItsRecordingPassFails fails the
// pass that records an earlier pass's first write of an instruction, at its
// update and at its commit: the stream's next pass records that write and
// does not write the instruction again.
func TestFirstWriteIsRecordedWithoutAResendWhenItsRecordingPassFails(t *testing.T) {
	for _, row := range []struct {
		name   string
		commit bool
	}{
		{"the update", false},
		{"the commit", true},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := newHubFixture(t, 720*time.Hour)
			_, stream := f.openOnline()
			f.clock.waitTimers(t, 2)
			// Stored and announced outside the hub's pool, so that only the
			// passes run the failing statement: the one that writes the
			// instruction after its commit, then the one that records it.
			instruction := f.storeWithoutNotice(stream.connectionID, "sess_failed_pass")
			var record *statementPause
			if row.commit {
				record = f.pauseOn("SET first_written_at")
			} else {
				f.failOn("SET first_written_at")
			}
			notice, _ := json.Marshal(instance.ConnectionNotice{WorkspaceID: string(f.ws), ConnectionID: stream.connectionID, IssueSeq: 1})
			if _, err := f.admin.ExecContext(f.ctx, `SELECT pg_notify($1,$2)`, instance.ConnectionNoticeChannel(hubTestPod), string(notice)); err != nil {
				t.Fatal(err)
			}
			f.requireInstructions(stream, instruction)
			if record != nil {
				<-record.reached
				f.failOn("commit")
				close(record.release)
			}
			f.clock.waitTimers(t, 3)
			if f.count(`SELECT count(*) FROM connection_instructions WHERE message_id=$1 AND first_written_at IS NULL`, instruction) != 1 {
				t.Fatal("the failed pass recorded the instruction's first write")
			}
			f.clock.Advance(passRetryFirst + passRetryFirst/5)
			waitUntil(t, func() bool {
				return f.count(`SELECT count(*) FROM connection_instructions WHERE message_id=$1 AND first_written_at IS NOT NULL`, instruction) == 1
			})
			requireNoEvent(t, stream)
		})
	}
}

// TestReportedInstructionStillRecordsItsSuccessfulWrite stores an
// instruction's report after a pass wrote it and before the next pass, which
// records that write, selects what to write: the write is still recorded,
// with one delivery span in the issuing trace, and is not repeated.
func TestReportedInstructionStillRecordsItsSuccessfulWrite(t *testing.T) {
	spans := tracetest.NewInMemoryExporter()
	t.Cleanup(workload.InstallTelemetry(workload.NewTelemetry(workload.TelemetryOptions{Service: ServiceName, SyncExporter: spans, SampleRatio: 1})))
	f := newHubFixture(t, 720*time.Hour)
	_, stream := f.openOnline()
	// A first instruction, written and recorded, shows that no earlier
	// pass is still running when the pause is set.
	warmup := f.issueInstruction(f.ctx, stream.connectionID, "sess_warmup")
	f.requireInstructions(stream, warmup)
	waitUntil(t, func() bool {
		return f.count(`SELECT count(*) FROM connection_instructions WHERE message_id=$1 AND first_written_at IS NOT NULL`, warmup) == 1
	})
	traced, span := workload.StartRequestSpan(f.ctx, "test.report", "")
	defer span.End()
	traceparent := workload.ContextTraceparent(traced)
	const selection = "reported_at IS NULL AND issue_seq<="
	writing := f.pauseOn(selection)
	instruction := f.issueInstruction(traced, stream.connectionID, "sess_reported")
	<-writing.reached
	// The next selection is that of the pass after the one that writes the
	// instruction, which records its write.
	recording := f.pauseOn(selection)
	close(writing.release)
	f.requireInstructions(stream, instruction)
	<-recording.reached
	f.markReported(stream.connectionID, instruction)
	if f.count(`SELECT count(*) FROM connection_instructions WHERE message_id=$1 AND first_written_at IS NOT NULL`, instruction) != 0 {
		t.Fatal("the write was recorded before the report was stored")
	}
	close(recording.release)
	sentinel := f.issueInstruction(traced, stream.connectionID, "sess_after_report")
	f.requireInstructions(stream, sentinel)
	awaitDeliverySpan(t, spans, sentinel)
	if f.count(`SELECT count(*) FROM connection_instructions WHERE message_id=$1 AND reported_at IS NOT NULL AND first_written_at IS NOT NULL`, instruction) != 1 {
		t.Fatal("the reported instruction's write was not recorded")
	}
	for _, messageID := range []string{instruction, sentinel} {
		delivered := deliverySpans(spans, messageID)
		if len(delivered) != 1 || delivered[0].SpanContext.TraceID().String() != traceparent[3:35] {
			t.Fatalf("delivery %s: %v; want exactly one in the issuing trace", messageID, delivered)
		}
	}
	// The three first writes, the warmup's included, are the histogram's
	// only samples.
	if count := metricValue(t, f.pods[hubTestPod].metrics, "computerhub_instructions_queue_wait_seconds_count", ""); count != 3 {
		t.Fatalf("queue-wait count = %v; want 3", count)
	}
	requireNoEvent(t, stream)
}

// deliverySpans selects only the original message, excluding progress sentinels.
func deliverySpans(spans *tracetest.InMemoryExporter, messageID string) []tracetest.SpanStub {
	var found []tracetest.SpanStub
	for _, stub := range spans.GetSpans() {
		if stub.Name != "instruction.deliver" {
			continue
		}
		for _, attr := range stub.Attributes {
			if string(attr.Key) == "message.id" && attr.Value.AsString() == messageID {
				found = append(found, stub)
				break
			}
		}
	}
	return found
}

func awaitDeliverySpan(t *testing.T, spans *tracetest.InMemoryExporter, messageID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for len(deliverySpans(spans, messageID)) == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("no delivery span for %s within 10 seconds", messageID)
		}
		time.Sleep(10 * time.Millisecond)
	}
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
	awaitDeliverySpan(t, spans, instruction)
	resumed := f.resume(b.url, cli, stream.connectionID)
	resumed.next()
	if event := resumed.next(); event.Envelope.Traceparent != protocol.Some(traceparent) || !bytes.Equal(event.Data, f.envelopeOf(instruction)) {
		t.Fatalf("resend %s; want the stored envelope with traceparent %s", event.Data, traceparent)
	}
	// Complete the resumed original's recording pass before counting its
	// observations. The later sentinel is sampled and processed serially.
	sentinel := f.issueInstruction(traced, resumed.connectionID, "sess_trace_barrier")
	f.requireInstructions(resumed, sentinel)
	awaitDeliverySpan(t, spans, sentinel)
	stream.ended()
	delivers := deliverySpans(spans, instruction)
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

// TestInstructionWithoutAStreamCountsAsQueued writes two instructions and
// stores a third once the stream is gone: two are in progress and one is
// queued, and the oldest queued age is the queued one's 30 seconds, though a
// written one is older.
func TestInstructionWithoutAStreamCountsAsQueued(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	_, stream := f.openOnline()
	oldest := f.issueInstruction(f.ctx, stream.connectionID, "sess_written")
	written := f.issueInstruction(f.ctx, stream.connectionID, "sess_written_too")
	f.requireInstructions(stream, oldest, written)
	waitUntil(t, func() bool {
		return f.count(`SELECT count(*) FROM connection_instructions WHERE message_id IN ($1,$2) AND first_written_at IS NOT NULL`, oldest, written) == 2
	})
	_ = stream.stream.Close()
	waitUntil(t, func() bool { return f.streams.lookup(stream.connectionID) == nil })
	waiting := f.issueInstruction(f.ctx, stream.connectionID, "sess_waiting")
	for messageID, age := range map[string]string{waiting: "30 seconds", oldest: "60 seconds"} {
		if _, err := f.admin.ExecContext(f.ctx, `UPDATE connection_instructions SET created_at=clock_timestamp()-$2::interval WHERE message_id=$1`, messageID, age); err != nil {
			t.Fatal(err)
		}
	}
	metrics, err := instructionPressure(f.client)(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]float64{}
	for _, metric := range metrics {
		values[metric.Name] = metric.Value
	}
	if values["computerhub_instructions_queued"] != 1 || values["computerhub_instructions_in_progress"] != 2 ||
		math.Abs(values["computerhub_instructions_oldest_queued_seconds"]-30) > 1 {
		t.Fatalf("pressure = %v; want two written and one queued 30 seconds ago, within a second", values)
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

// streamEnds is computerhub_streams_ended_total of metrics, by cause, less
// since, an earlier reading.
func streamEnds(t *testing.T, metrics *hubMetrics, since map[string]float64) map[string]float64 {
	t.Helper()
	samples, err := metrics.registry.Collector()(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ends := map[string]float64{}
	for _, sample := range samples {
		if sample.Name == "computerhub_streams_ended_total" && len(sample.Labels) == 1 {
			if value := sample.Value - since[sample.Labels[0].Value]; value != 0 {
				ends[sample.Labels[0].Value] = value
			}
		}
	}
	return ends
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
	f.restartChannelListener(instance.ConnectionNoticeChannel(pod))
}

// restartChannelListener ends the one LISTEN connection on channel, which its
// listener then opens again.
func (f *hubFixture) restartChannelListener(channel string) {
	f.t.Helper()
	listen := `LISTEN ` + channel
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
