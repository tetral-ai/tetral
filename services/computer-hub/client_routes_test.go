package computerhub

import (
	"bytes"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/tetral-ai/tetral/integration/enginetest"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workload"
)

func TestEventsRefusesEachCredentialFailureAndWritesNoConnection(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	mine, other := f.userInstance(time.Time{}), f.userInstance(time.Time{})
	revoked := f.secondCredential(mine, time.Time{})
	if status, _ := f.do(http.MethodDelete, "/v1/instances/"+mine.InstanceID+"/credentials/"+revoked.CredentialID, nil, ""); status != http.StatusNoContent {
		t.Fatalf("revoke = %d", status)
	}
	expired := f.userInstance(time.Time{})
	if _, err := f.admin.ExecContext(f.ctx, `UPDATE instance_credentials SET issued_at=clock_timestamp()-interval '2 hours',expires_at=clock_timestamp()-interval '1 hour' WHERE id=$1`, expired.CredentialID); err != nil {
		t.Fatal(err)
	}
	deregistered := f.userInstance(time.Time{})
	if status, _ := f.do(http.MethodDelete, "/v1/instances/"+deregistered.InstanceID, nil, ""); status != http.StatusNoContent {
		t.Fatalf("deregister = %d", status)
	}
	otherInstance := f.cli(mine.InstanceID, mine.CredentialID)
	otherInstance.InstanceID, otherInstance.CredentialID = other.InstanceID, other.CredentialID
	unknown := f.cli(mine.InstanceID, "ccred_unknown")
	for _, tc := range []struct {
		name string
		cli  *enginetest.CLI
		code protocol.ErrorCode
	}{
		{"key of another instance", otherInstance, protocol.ErrorCredentialInvalid},
		{"unknown key", unknown, protocol.ErrorCredentialInvalid},
		{"revoked credential", revoked, protocol.ErrorCredentialRevoked},
		{"expired credential", expired, protocol.ErrorCredentialExpired},
		{"deregistered instance", deregistered, protocol.ErrorInstanceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream, response, err := tc.cli.OpenEvents(f.ctx, tc.cli.EventsQuery("", nil))
			if stream != nil {
				t.Fatal("the stream opened")
			}
			requireClientError(t, response, err, tc.code)
		})
	}
	if n := f.count(`SELECT count(*) FROM instance_connections`); n != 0 {
		t.Fatalf("connection rows = %d; want 0", n)
	}
}

func TestReadyAndHeartbeatGrantsAreCappedByTheCredential(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	short := f.userInstance(time.Now().Add(10 * time.Second))
	stream := f.open(short)
	grant := f.acceptReady(short, stream, "ready_1")
	row := f.connectionRow(stream.connectionID)
	credentialExpiry := f.credentialExpiry(short.CredentialID)
	if grant.ValidForMS <= 0 || grant.ValidForMS > 10_000 || parseTime(t, row["lease_expires_at"]).After(credentialExpiry) {
		t.Fatalf("grant %d ms, lease %v; want at most 10,000 ms and the lease at most the expiry %v", grant.ValidForMS, row["lease_expires_at"], credentialExpiry)
	}
	f.clock.waitTimers(t, 2)
	f.clock.Advance(HeartbeatInterval)
	report := stream.heartbeat("heartbeat_1")
	time.Sleep(time.Until(credentialExpiry) + 100*time.Millisecond)
	response, err := short.Heartbeat(f.ctx, report)
	requireClientError(t, response, err, protocol.ErrorCredentialExpired)
	if parseTime(t, f.connectionRow(stream.connectionID)["lease_expires_at"]).After(credentialExpiry) {
		t.Fatal("the lease passed the credential's expiry")
	}

	long := f.userInstance(time.Now().Add(time.Hour))
	longStream := f.open(long)
	pending := f.clock.waiting()
	if grant := f.acceptReady(long, longStream, "ready_1"); grant.ValidForMS <= 0 || grant.ValidForMS > 45_000 {
		t.Fatalf("ready grant = %d ms; want at most 45,000", grant.ValidForMS)
	}
	f.clock.waitTimers(t, pending+1)
	f.clock.Advance(HeartbeatInterval)
	response, err = long.Heartbeat(f.ctx, longStream.heartbeat("heartbeat_1"))
	if err != nil {
		t.Fatal(err)
	}
	if grant, err := response.Grant(); err != nil || grant.ValidForMS <= 0 || grant.ValidForMS > 45_000 {
		t.Fatalf("heartbeat grant = %+v, %v; want at most 45,000 ms", grant, err)
	}
}

func TestReplayedOrRefusedReadyAndHeartbeatRenewNothing(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	cli := f.userInstance(time.Time{})
	stream := f.open(cli)
	ready := readyOf(stream.connectionID, "ready_1", "1.0.0")
	readyAnswer, err := cli.Ready(f.ctx, ready)
	if err != nil || readyAnswer.Status != http.StatusOK {
		t.Fatalf("ready = %d %s, %v", readyAnswer.Status, readyAnswer.Body, err)
	}
	f.clock.waitTimers(t, 2)
	f.clock.Advance(HeartbeatInterval)
	heartbeat := stream.heartbeat("heartbeat_1")
	heartbeatAnswer, err := cli.Heartbeat(f.ctx, heartbeat)
	if err != nil || heartbeatAnswer.Status != http.StatusOK {
		t.Fatalf("heartbeat = %d %s, %v", heartbeatAnswer.Status, heartbeatAnswer.Body, err)
	}
	before := f.connectionRow(stream.connectionID)
	time.Sleep(1100 * time.Millisecond)
	for name, replay := range map[string]struct {
		send     func() (enginetest.Response, error)
		original enginetest.Response
	}{
		"ready":     {func() (enginetest.Response, error) { return cli.Ready(f.ctx, ready) }, readyAnswer},
		"heartbeat": {func() (enginetest.Response, error) { return cli.Heartbeat(f.ctx, heartbeat) }, heartbeatAnswer},
	} {
		response, err := replay.send()
		if err != nil || response.Status != http.StatusOK || !bytes.Equal(response.Body, replay.original.Body) {
			t.Fatalf("%s replay = %d %s, %v; want the original %s", name, response.Status, response.Body, err, replay.original.Body)
		}
	}
	requireRowUnchanged(t, before, f.connectionRow(stream.connectionID))

	staleHeartbeat := heartbeat
	staleHeartbeat.MessageID = "heartbeat_2"
	for name, send := range map[string]func() (enginetest.Response, error){
		"second ready": func() (enginetest.Response, error) {
			return cli.Ready(f.ctx, readyOf(stream.connectionID, "ready_2", "1.0.0"))
		},
		"heartbeat with the last seq": func() (enginetest.Response, error) { return cli.Heartbeat(f.ctx, staleHeartbeat) },
	} {
		response, err := send()
		requireClientError(t, response, err, protocol.ErrorInvalidMessage)
		requireRowUnchanged(t, before, f.connectionRow(stream.connectionID))
		t.Logf("%s refused", name)
	}

	early := f.userInstance(time.Time{})
	earlyStream := f.open(early)
	earlyBefore := f.connectionRow(earlyStream.connectionID)
	response, err := early.Heartbeat(f.ctx, protocol.Heartbeat{ConnectionID: earlyStream.connectionID, MessageID: "heartbeat_1",
		RequestMessageID: instance.HeartbeatMessageID(earlyStream.connectionID, instance.HeartbeatSeqStride+1), HeartbeatSeq: instance.HeartbeatSeqStride + 1})
	requireClientError(t, response, err, protocol.ErrorInvalidMessage)
	requireRowUnchanged(t, earlyBefore, f.connectionRow(earlyStream.connectionID))
}

func TestReportOnAnEndedConnectionIsStale(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	first := f.userInstance(time.Now().Add(2 * time.Second))
	second := f.secondCredential(first, time.Time{})
	ended := f.open(first)
	time.Sleep(2100 * time.Millisecond)
	f.open(second)
	row := f.connectionRow(ended.connectionID)
	if row["end_cause"] != instance.EndLeaseExpired {
		t.Fatalf("first connection = %v; want ended with lease_expired", row)
	}
	counts := [2]int{f.count(`SELECT count(*) FROM connection_protocol_errors`), f.count(`SELECT count(*) FROM connection_upstream_messages`)}
	response, err := second.ProtocolError(f.ctx, protocol.ProtocolErrorReport{ConnectionID: ended.connectionID, MessageID: "report_1", Error: protocol.ProtocolError{Code: "invalid_message", Message: "unparsable"}})
	requireClientError(t, response, err, protocol.ErrorStaleConnection)
	response, err = second.Heartbeat(f.ctx, protocol.Heartbeat{ConnectionID: ended.connectionID, MessageID: "heartbeat_1",
		RequestMessageID: instance.HeartbeatMessageID(ended.connectionID, instance.HeartbeatSeqStride+1), HeartbeatSeq: instance.HeartbeatSeqStride + 1})
	requireClientError(t, response, err, protocol.ErrorStaleConnection)
	requireRowUnchanged(t, row, f.connectionRow(ended.connectionID))
	if after := [2]int{f.count(`SELECT count(*) FROM connection_protocol_errors`), f.count(`SELECT count(*) FROM connection_upstream_messages`)}; after != counts {
		t.Fatalf("report and ledger rows = %v; want %v", after, counts)
	}
}

func TestProtocolErrorReportReplaysAndConflicts(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	cli := f.userInstance(time.Time{})
	stream := f.open(cli)
	report := protocol.ProtocolErrorReport{ConnectionID: stream.connectionID, MessageID: "report_1", RequestMessageID: protocol.Some("msg_never_sent"),
		Error: protocol.ProtocolError{Code: "invalid_message", Message: "REPORT_TEXT_SENTINEL"}}
	first, err := cli.ProtocolError(f.ctx, report)
	if err != nil {
		t.Fatal(err)
	}
	if ack, err := first.Acknowledgement(); err != nil || ack.MessageID != "report_1" || ack.ConnectionID != stream.connectionID {
		t.Fatalf("report = %d %s, %v", first.Status, first.Body, err)
	}
	time.Sleep(1100 * time.Millisecond)
	replay, err := cli.ProtocolError(f.ctx, report)
	if err != nil || replay.Status != http.StatusOK || !bytes.Equal(replay.Body, first.Body) {
		t.Fatalf("replay = %d %s, %v; want %s", replay.Status, replay.Body, err, first.Body)
	}
	rows := func() string {
		var data []byte
		if err := f.admin.QueryRowContext(f.ctx, `SELECT coalesce(json_agg(row_to_json(e)),'[]') FROM connection_protocol_errors e`).Scan(&data); err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	stored := rows()
	var parsed []map[string]any
	decodeJSON(t, []byte(stored), &parsed)
	if len(parsed) != 1 || parsed[0]["code"] != "invalid_message" || parsed[0]["request_message_id"] != "msg_never_sent" || parsed[0]["message_id"] != "report_1" {
		t.Fatalf("protocol error rows = %s; want the one report as given", stored)
	}
	changed := report
	changed.Error.Code = "message_conflict"
	response, err := cli.ProtocolError(f.ctx, changed)
	requireClientError(t, response, err, protocol.ErrorMessageConflict)
	if rows() != stored {
		t.Fatalf("protocol error rows changed: %s", rows())
	}
	logs := f.logs.String()
	if strings.Count(logs, `"event":"client.protocol_error"`) != 1 || strings.Contains(logs, "REPORT_TEXT_SENTINEL") ||
		!strings.Contains(logs, `"request.message.id":"msg_never_sent"`) || !strings.Contains(logs, `"message.id":"report_1"`) {
		t.Fatalf("logs hold %d client.protocol_error records or the report's message", strings.Count(logs, `"event":"client.protocol_error"`))
	}
	withNUL := protocol.ProtocolErrorReport{ConnectionID: stream.connectionID, MessageID: "report_nul", Error: protocol.ProtocolError{Code: "invalid_message", Message: "a\u0000b"}}
	if response, err := cli.ProtocolError(f.ctx, withNUL); err != nil || response.Status != http.StatusOK {
		t.Fatalf("report holding U+0000 = %d %s, %v; want its ACK", response.Status, response.Body, err)
	}
	var message []byte
	if err := f.admin.QueryRowContext(f.ctx, `SELECT message FROM connection_protocol_errors WHERE message_id='report_nul'`).Scan(&message); err != nil || string(message) != "a\u0000b" {
		t.Fatalf("stored message %q, %v; want the report's bytes", message, err)
	}
}

// TestHeartbeatsRecordNothingAndEventStreamsStartNoSpan proves the hub's
// quiet and untraced client requests: a heartbeat writes no request record
// and starts no span, an event stream writes its record and starts no span,
// and a ready writes both.
func TestHeartbeatsRecordNothingAndEventStreamsStartNoSpan(t *testing.T) {
	spans := tracetest.NewInMemoryExporter()
	t.Cleanup(workload.InstallTelemetry(workload.NewTelemetry(workload.TelemetryOptions{Service: ServiceName, SyncExporter: spans, SampleRatio: 1})))
	f := newHubFixture(t, 720*time.Hour)
	cli, stream := f.openOnline()
	f.clock.waitTimers(t, 2)
	f.clock.Advance(HeartbeatInterval)
	beat := stream.heartbeat("msg_quiet_beat")
	if response, err := cli.Heartbeat(f.ctx, beat); err != nil || response.Status != http.StatusOK {
		t.Fatalf("heartbeat = %d %s, %v", response.Status, response.Body, err)
	}
	_ = stream.stream.Close()
	recorded := func(route string) bool {
		return strings.Contains(f.logs.String(), `"http.route":"`+route+`"`)
	}
	waitUntil(t, func() bool { return recorded(routeClientEvents) })
	if !recorded(routeClientReady) || recorded(routeClientHeartbeat) {
		t.Fatal("want request records of the ready and the events stream, none of the heartbeat")
	}
	var names []string
	for _, stub := range spans.GetSpans() {
		names = append(names, stub.Name)
	}
	if !slices.Contains(names, "POST "+routeClientReady) || slices.Contains(names, "POST "+routeClientHeartbeat) || slices.Contains(names, "GET "+routeClientEvents) {
		t.Fatalf("spans = %v; want the ready's and neither the heartbeat's nor the events stream's", names)
	}
}

// TestConcurrentCopiesOfAMessageGetTheFirstOnesAnswer holds the first copy
// of a ready and of a protocol-error report at its write while a second copy
// with the same message_id and content waits for the first's locks: once the
// first commits, the second answers its ACK byte for byte.
func TestConcurrentCopiesOfAMessageGetTheFirstOnesAnswer(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	type answer struct {
		response enginetest.Response
		err      error
	}
	for _, row := range []struct {
		name, hold string
		send       func(cli *enginetest.CLI, connectionID string) (enginetest.Response, error)
	}{
		{"ready", "UPDATE instances SET cli_version", func(cli *enginetest.CLI, connectionID string) (enginetest.Response, error) {
			return cli.Ready(f.ctx, readyOf(connectionID, "ready_copy", "1.0.0"))
		}},
		{"protocol-error report", "INSERT INTO connection_protocol_errors", func(cli *enginetest.CLI, connectionID string) (enginetest.Response, error) {
			return cli.ProtocolError(f.ctx, protocol.ProtocolErrorReport{ConnectionID: connectionID, MessageID: "report_copy",
				Error: protocol.ProtocolError{Code: "invalid_message", Message: "copy"}})
		}},
	} {
		t.Run(row.name, func(t *testing.T) {
			cli := f.userInstance(time.Time{})
			stream := f.open(cli)
			pause := f.pauseOn(row.hold)
			release := sync.OnceFunc(func() { close(pause.release) })
			defer release()
			send := func() <-chan answer {
				done := make(chan answer, 1)
				go func() {
					response, err := row.send(cli, stream.connectionID)
					done <- answer{response, err}
				}()
				return done
			}
			first := send()
			<-pause.reached
			second := send()
			f.awaitLockWait()
			release()
			a, b := <-first, <-second
			if a.err != nil || b.err != nil || a.response.Status != http.StatusOK || b.response.Status != http.StatusOK || !bytes.Equal(a.response.Body, b.response.Body) {
				t.Fatalf("copies = %d %s (%v) and %d %s (%v); want one ACK twice", a.response.Status, a.response.Body, a.err, b.response.Status, b.response.Body, b.err)
			}
		})
	}
}

func TestProtocolVersionTwoIsInvalidAndUnknownOptionalFieldsAreAccepted(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	cli := f.userInstance(time.Time{})
	query := cli.EventsQuery("", nil)
	query.Set("protocol_version", "2")
	stream, response, err := cli.OpenEvents(f.ctx, query)
	if stream != nil {
		t.Fatal("the stream opened")
	}
	requireClientError(t, response, err, protocol.ErrorInvalidMessage)
	if n := f.count(`SELECT count(*) FROM instance_connections`); n != 0 {
		t.Fatalf("connection rows = %d; want 0", n)
	}
	opened := f.open(cli)
	readyWith := func(change func(map[string]any)) []byte {
		var body map[string]any
		decodeJSON(t, protocol.Encode(readyOf(opened.connectionID, "ready_1", "1.0.0")), &body)
		change(body)
		return mustJSON(t, body)
	}
	response, err = cli.Post(f.ctx, "/v1/client/ready", readyWith(func(b map[string]any) { b["protocol_version"] = 2 }))
	requireClientError(t, response, err, protocol.ErrorInvalidMessage)
	response, err = cli.Post(f.ctx, "/v1/client/ready", readyWith(func(b map[string]any) { b["future_member"] = map[string]any{"a": 1} }))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := response.Grant(); err != nil {
		t.Fatalf("ready with an unknown optional member: %v", err)
	}
}

func TestReadyBelowTheMinimumVersionIsRefusedAndShownOffline(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour, withCLIVersions("1.0.0", "1.0.0"))
	cli := f.userInstance(time.Time{})
	stream := f.open(cli)
	before := f.connectionRow(stream.connectionID)
	response, err := cli.Ready(f.ctx, readyOf(stream.connectionID, "ready_1", "0.9.0"))
	requireClientError(t, response, err, protocol.ErrorCLIUpgradeRequired)
	object := f.instanceRead(cli.InstanceID)
	connection := object["connection"].(map[string]any)
	connectionError, _ := connection["error"].(map[string]any)
	if connection["state"] != instance.ConnectionOffline || connectionError["code"] != "cli_upgrade_required" {
		t.Fatalf("connection = %v; want offline with cli_upgrade_required", connection)
	}
	if object["cli_version"] != "0.9.0" || object["cli_update_available"] != true {
		t.Fatalf("instance = %v; want cli_version 0.9.0 with an update available", object)
	}
	if after := f.connectionRow(stream.connectionID); after["lease_expires_at"] != before["lease_expires_at"] {
		t.Fatalf("lease %v -> %v; the refusal renewed it", before["lease_expires_at"], after["lease_expires_at"])
	}
}

// TestReadyAcceptsACLIVersionOfAnyLength sends a ready whose version, a
// SemVer with 300 characters of build metadata, has no length bound.
func TestReadyAcceptsACLIVersionOfAnyLength(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	cli := f.userInstance(time.Time{})
	stream := f.open(cli)
	version := "1.0.0+" + strings.Repeat("a", 300)
	response, err := cli.Ready(f.ctx, readyOf(stream.connectionID, "ready_long_version", version))
	if _, decodeErr := response.Grant(); err != nil || decodeErr != nil {
		t.Fatalf("ready = %d %s, %v; want its grant", response.Status, response.Body, err)
	}
	if got := f.instanceRead(cli.InstanceID)["cli_version"]; got != version {
		t.Fatalf("cli_version = %v; want the ready's", got)
	}
}

func TestInstanceReadsFollowTheConnectionStates(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	waiting := f.userInstance(time.Now().Add(2 * time.Second))
	replacement := f.secondCredential(waiting, time.Time{})
	unanswered := f.open(waiting)
	requireConnectionObject(t, f.connectionState(waiting.InstanceID), unanswered.connectionID, waiting.CredentialID, instance.ConnectionRegistering)
	time.Sleep(2100 * time.Millisecond)
	requireConnectionObject(t, f.connectionState(waiting.InstanceID), unanswered.connectionID, waiting.CredentialID, instance.ConnectionRegistering)
	next := f.open(replacement)
	if row := f.connectionRow(unanswered.connectionID); row["end_cause"] != instance.EndLeaseExpired || row["ended_at"] == nil {
		t.Fatalf("replaced connection = %v; want ended with lease_expired", row)
	}
	requireConnectionObject(t, f.connectionState(waiting.InstanceID), next.connectionID, replacement.CredentialID, instance.ConnectionRegistering)

	online := f.userInstance(time.Now().Add(3 * time.Second))
	stream := f.open(online)
	grant := f.acceptReady(online, stream, "ready_1")
	granted := time.Now()
	connection := f.connectionState(online.InstanceID)
	requireConnectionObject(t, connection, stream.connectionID, online.CredentialID, instance.ConnectionOnline)
	if connection["ready_at"] == nil || string(mustJSON(t, connection["capabilities"])) != `{"drives":{"mount":true},"session_preparation":true}` {
		t.Fatalf("online connection = %v; want ready_at and the ready's capabilities", connection)
	}
	if !f.credentialInUse(online.InstanceID, online.CredentialID) {
		t.Fatal("the live connection's credential is not in_use")
	}
	time.Sleep(time.Until(granted.Add(time.Duration(grant.ValidForMS)*time.Millisecond)) + 200*time.Millisecond)
	requireConnectionObject(t, f.connectionState(online.InstanceID), stream.connectionID, online.CredentialID, instance.ConnectionOffline)
	if f.credentialInUse(online.InstanceID, online.CredentialID) {
		t.Fatal("the credential of an offline connection is in_use")
	}
}

func TestReadyBodyLimitIs65536Bytes(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	for _, tc := range []struct {
		size   int
		status int
	}{{protocol.DefaultMessageJSONBytes, http.StatusOK}, {protocol.DefaultMessageJSONBytes + 1, http.StatusRequestEntityTooLarge}} {
		cli := f.userInstance(time.Time{})
		stream := f.open(cli)
		body := paddedReady(t, readyOf(stream.connectionID, "ready_1", "1.0.0"), tc.size)
		response, err := cli.Post(f.ctx, "/v1/client/ready", body)
		if err != nil || response.Status != tc.status {
			t.Fatalf("%d-byte ready = %d %s, %v; want %d", len(body), response.Status, response.Body, err, tc.status)
		}
		row := f.connectionRow(stream.connectionID)
		if tc.status == http.StatusOK {
			if row["ready_accepted_at"] == nil {
				t.Fatal("the ready at the limit was not accepted")
			}
			continue
		}
		requireClientError(t, response, nil, protocol.ErrorRequestTooLarge)
		if row["ready_message_id"] != nil || f.count(`SELECT count(*) FROM connection_upstream_messages`) != 0 {
			t.Fatalf("the refused ready was recorded: %v", row)
		}
	}
}

func TestHeartbeatsFollowTheAcceptedReadyOnTheStreamClock(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	cli := f.userInstance(time.Time{})
	stream := f.open(cli)
	if n := f.clock.waiting(); n != 1 {
		t.Fatalf("pending timers before the ready = %d; want only the stream's age limit", n)
	}
	f.clock.Advance(30 * time.Second)
	f.acceptReady(cli, stream, "ready_1")
	f.clock.waitTimers(t, 2)
	requireNoEvent(t, stream)
	f.clock.Advance(14 * time.Second)
	requireNoEvent(t, stream)
	var seqs []int64
	for range 2 {
		f.clock.Advance(time.Second)
		report := stream.heartbeat("unused")
		if report.RequestMessageID != instance.HeartbeatMessageID(stream.connectionID, report.HeartbeatSeq) {
			t.Fatalf("heartbeat message_id %s for seq %d", report.RequestMessageID, report.HeartbeatSeq)
		}
		seqs = append(seqs, report.HeartbeatSeq)
		f.clock.waitTimers(t, 2)
		f.clock.Advance(14 * time.Second)
	}
	if seqs[0] != instance.HeartbeatSeqStride+1 || seqs[1] != instance.HeartbeatSeqStride+2 {
		t.Fatalf("first stream heartbeat_seq = %v; want %d and %d", seqs, instance.HeartbeatSeqStride+1, instance.HeartbeatSeqStride+2)
	}

	// The resumed stream of the accepted connection.
	later := f.resume(f.url, cli, stream.connectionID)
	first := later.next()
	heartbeat, ok := first.Envelope.Payload.(protocol.HeartbeatPayload)
	if !ok || heartbeat.HeartbeatSeq != 2*instance.HeartbeatSeqStride+1 || heartbeat.HeartbeatSeq <= slices.Max(seqs) {
		t.Fatalf("second stream's first event %s; want heartbeat %d with no clock advance", first.Data, 2*instance.HeartbeatSeqStride+1)
	}
}

func TestRevocationAndDeregistrationEndTheLiveConnection(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	for _, tc := range []struct {
		name  string
		end   func(cli *enginetest.CLI) string
		cause string
		code  protocol.ErrorCode
	}{
		{"revocation", func(cli *enginetest.CLI) string {
			return "/v1/instances/" + cli.InstanceID + "/credentials/" + cli.CredentialID
		}, instance.EndRevoked, protocol.ErrorCredentialRevoked},
		{"deregistration", func(cli *enginetest.CLI) string { return "/v1/instances/" + cli.InstanceID }, instance.EndDeregistered, protocol.ErrorInstanceUnavailable},
	} {
		func() {
			cli := f.userInstance(time.Time{})
			stream := f.open(cli)
			pending := f.clock.waiting()
			f.acceptReady(cli, stream, "ready_1")
			f.clock.waitTimers(t, pending+1)
			f.clock.Advance(HeartbeatInterval)
			report := stream.heartbeat("heartbeat_1")
			if status, _ := f.do(http.MethodDelete, tc.end(cli), nil, ""); status != http.StatusNoContent {
				t.Fatalf("delete = %d", status)
			}
			row := f.connectionRow(stream.connectionID)
			if row["end_cause"] != tc.cause || row["ended_at"] == nil {
				t.Fatalf("%s: connection after the delete = %v; want ended with %s", tc.name, row, tc.cause)
			}
			events := stream.ended()
			if len(events) != 1 {
				t.Fatalf("%s: events after the delete = %d; want one disconnect", tc.name, len(events))
			}
			disconnect, ok := events[0].Envelope.Payload.(protocol.DisconnectPayload)
			if !ok || disconnect.Scope != "connection" || disconnect.Retryable != protocol.Some(false) || disconnect.Code != protocol.Some(string(tc.code)) {
				t.Fatalf("%s: last event %s; want a connection disconnect with %s", tc.name, events[0].Data, tc.code)
			}
			response, err := cli.Heartbeat(f.ctx, report)
			if err != nil || response.Status == http.StatusOK {
				t.Fatalf("%s: heartbeat after the end = %d %s, %v; want a refusal", tc.name, response.Status, response.Body, err)
			}
			requireRowUnchanged(t, row, f.connectionRow(stream.connectionID))
		}()
	}
}

// TestRevocationWaitsForAGrantUnderTheConnectionsCredential holds a ready
// and a heartbeat that the instance's second active credential sends on the
// connection, at their connection lock, while the connection's credential is
// revoked: the revocation waits for the credential the request already
// locked, the grant commits, then the revocation ends the connection.
func TestRevocationWaitsForAGrantUnderTheConnectionsCredential(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	type answer struct {
		response enginetest.Response
		err      error
	}
	for _, row := range []struct {
		name  string
		ready bool
		send  func(other *enginetest.CLI, connectionID string) (enginetest.Response, error)
	}{
		{"ready", false, func(other *enginetest.CLI, connectionID string) (enginetest.Response, error) {
			return other.Ready(f.ctx, readyOf(connectionID, "ready_other", "1.0.0"))
		}},
		{"heartbeat", true, func(other *enginetest.CLI, connectionID string) (enginetest.Response, error) {
			return other.Heartbeat(f.ctx, protocol.Heartbeat{ConnectionID: connectionID, MessageID: "heartbeat_other",
				RequestMessageID: instance.HeartbeatMessageID(connectionID, 1), HeartbeatSeq: 1})
		}},
	} {
		t.Run(row.name, func(t *testing.T) {
			cli := f.userInstance(time.Time{})
			other := f.secondCredential(cli, time.Time{})
			stream := f.open(cli)
			if row.ready {
				f.acceptReady(cli, stream, "ready_1")
			}
			pause := f.pauseOn("AND lease_expires_at>$4 FOR UPDATE")
			release := sync.OnceFunc(func() { close(pause.release) })
			defer release()
			granted := make(chan answer, 1)
			go func() {
				response, err := row.send(other, stream.connectionID)
				granted <- answer{response, err}
			}()
			<-pause.reached
			revoked := make(chan int, 1)
			go func() {
				status, _ := f.do(http.MethodDelete, "/v1/instances/"+cli.InstanceID+"/credentials/"+cli.CredentialID, nil, "")
				revoked <- status
			}()
			f.awaitLockWait()
			release()
			if a := <-granted; a.err != nil || a.response.Status != http.StatusOK {
				t.Fatalf("%s of the second credential = %d %s, %v; want its grant", row.name, a.response.Status, a.response.Body, a.err)
			}
			if status := <-revoked; status != http.StatusNoContent {
				t.Fatalf("revocation of the connection's credential = %d; want 204", status)
			}
			if connection := f.connectionRow(stream.connectionID); connection["end_cause"] != instance.EndRevoked {
				t.Fatalf("connection after the revocation = %v; want ended with %s", connection, instance.EndRevoked)
			}
		})
	}
}

func requireNoEvent(t *testing.T, stream *testStream) {
	t.Helper()
	select {
	case event := <-stream.events:
		t.Fatalf("unexpected event %s", event.Data)
	case <-time.After(150 * time.Millisecond):
	}
}

func requireRowUnchanged(t *testing.T, before, after map[string]any) {
	t.Helper()
	if string(mustJSON(t, before)) != string(mustJSON(t, after)) {
		t.Fatalf("connection row changed:\n%v\n%v", before, after)
	}
}

func requireConnectionObject(t *testing.T, connection map[string]any, connectionID, credentialID, state string) {
	t.Helper()
	fields := []string{"capabilities", "credential_id", "error", "id", "last_heartbeat_at", "lease_expires_at", "ready_at", "state"}
	var got []string
	for field := range connection {
		got = append(got, field)
	}
	slices.Sort(got)
	if !slices.Equal(got, fields) || connection["id"] != connectionID || connection["credential_id"] != credentialID || connection["state"] != state {
		t.Fatalf("connection = %v; want %s of %s in state %s with exactly %v", connection, connectionID, credentialID, state, fields)
	}
}

func (f *hubFixture) credentialExpiry(credentialID string) time.Time {
	f.t.Helper()
	var expiresAt time.Time
	if err := f.admin.QueryRowContext(f.ctx, `SELECT expires_at FROM instance_credentials WHERE id=$1`, credentialID).Scan(&expiresAt); err != nil {
		f.t.Fatal(err)
	}
	return expiresAt
}

func (f *hubFixture) credentialInUse(instanceID, credentialID string) bool {
	f.t.Helper()
	status, data := f.do(http.MethodGet, "/v1/instances/"+instanceID+"/credentials", nil, "")
	if status != http.StatusOK {
		f.t.Fatalf("credential list = %d", status)
	}
	var list struct {
		Data []map[string]any `json:"data"`
	}
	decodeJSON(f.t, data, &list)
	for _, credential := range list.Data {
		if credential["id"] == credentialID {
			return credential["in_use"] == true
		}
	}
	f.t.Fatalf("credential %s not listed", credentialID)
	return false
}

func parseTime(t *testing.T, value any) time.Time {
	t.Helper()
	text, _ := value.(string)
	at, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		t.Fatalf("time %v: %v", value, err)
	}
	return at
}

// paddedReady encodes ready with an unknown optional member that brings the
// body to exactly size bytes.
func paddedReady(t *testing.T, ready protocol.Ready, size int) []byte {
	t.Helper()
	body := protocol.Encode(ready)
	prefix := append(append([]byte(nil), body[:len(body)-1]...), `,"padding":"`...)
	padding := size - len(prefix) - len(`"}`)
	out := append(append(prefix, bytes.Repeat([]byte("x"), padding)...), `"}`...)
	if len(out) != size || !json.Valid(out) {
		t.Fatalf("padded ready of %d bytes", len(out))
	}
	return out
}
