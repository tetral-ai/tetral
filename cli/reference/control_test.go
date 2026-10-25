package reference

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"testing"
	"time"
)

func TestCredentialControlAddsRevokesAndExpiresCredentials(t *testing.T) {
	ts := startServer(t)
	ts.call("credential", map[string]any{"action": "add", "credential": Credential{
		InstanceID: "inst_c", CredentialID: "cred_c", ConnectionKey: "tetral_ck_c", ExpiresAt: testStart.Add(time.Hour), State: "active",
	}}, nil)
	stream := ts.mustOpen("tetral_ck_c", "inst_c", "cred_c", nil)
	stream.expect("connected")
	ts.call("credential", map[string]any{"action": "revoke", "credential_id": "cred_c"}, nil)
	if _, refused := ts.open("tetral_ck_c", "inst_c", "cred_c", nil); refused.code() != "credential_revoked" {
		t.Errorf("after revoke: %d %s", refused.status, refused.body)
	}
	ts.call("credential", map[string]any{"action": "expire", "credential_id": "cred_b"}, nil)
	if _, refused := ts.open("tetral_ck_b", "inst_b", "cred_b", nil); refused.code() != "credential_expired" {
		t.Errorf("after expire: %d %s", refused.status, refused.body)
	}
}

func TestPrepareControlIssuesWithinTheStagingLimit(t *testing.T) {
	ts := startServer(t)
	stream := ts.mustOpen("tetral_ck_a", "inst_a", "cred_a", nil)
	connectionID := stream.expect("connected")["connection_id"].(string)
	var first string
	for index := range StagingLimit + 1 {
		messageID := ts.prepare("sess_"+twoDigits(index), "bind_"+twoDigits(index))
		if index == 0 {
			first = messageID
		}
	}
	if conn := ts.connection(connectionID); conn.Queued != StagingLimit+1 || len(conn.Issued) != 0 {
		t.Fatalf("before the ready: issued %d, queued %d; want every instruction queued", len(conn.Issued), conn.Queued)
	}
	ts.post("ready", "tetral_ck_a", readyBody(connectionID, "ready_1"))
	if sent := stream.expect("session.prepare"); sent["message_id"] != first {
		t.Fatalf("first instruction %v; want the first one asked for", sent)
	}
	for range StagingLimit - 1 {
		stream.expect("session.prepare")
	}
	ts.prepare("sess_65", "bind_65")
	if conn := ts.connection(connectionID); conn.Queued != 2 || len(conn.Issued) != StagingLimit {
		t.Fatalf("issued %d, queued %d; want %d and 2", len(conn.Issued), conn.Queued, StagingLimit)
	}
	if answer := ts.post("session-status", "tetral_ck_a", preparedReport(connectionID, "report_0", first, "sess_00", "bind_00")); answer.status != http.StatusOK {
		t.Fatalf("the first report: %d %s", answer.status, answer.body)
	}
	for _, binding := range []string{"bind_64", "bind_65"} {
		if released := stream.expect("session.prepare"); payload(released)["binding_id"] != binding {
			t.Errorf("released %v; want %s", released, binding)
		}
	}
}

func TestCloseControlIssuesACloseThatItsReportAnswers(t *testing.T) {
	ts := startServer(t)
	stream, connectionID := ts.online()
	ts.prepare("sess_1", "bind_1")
	stream.expect("session.prepare")
	var issued struct {
		MessageID string `json:"message_id"`
	}
	ts.call("close", map[string]any{"instance_id": "inst_a", "payload": map[string]any{"session_id": "sess_1", "binding_id": "bind_1"}}, &issued)
	closed := stream.expect("session.close")
	if closed["message_id"] != issued.MessageID || payload(closed)["session_id"] != "sess_1" {
		t.Fatalf("close %v", closed)
	}
	report := preparedReport(connectionID, "report_1", issued.MessageID, "sess_1", "bind_1")
	report["status"] = "closed"
	delete(report, "cwd")
	if answer := ts.post("session-status", "tetral_ck_a", report); answer.status != http.StatusOK || answer.member("accepted") != true {
		t.Errorf("closed report: %d %s", answer.status, answer.body)
	}
}

func TestSendControlWritesAnyEventOrADuplicate(t *testing.T) {
	ts := startServer(t)
	stream, _ := ts.online()
	malformed := `{"protocol_version":1,"type":"session.close"`
	ts.call("send", map[string]any{"instance_id": "inst_a", "data": malformed}, nil)
	if event := stream.next(); event["raw"] != malformed {
		t.Errorf("malformed event %v", event)
	}
	prepare := ts.prepare("sess_1", "bind_1")
	original := stream.expect("session.prepare")
	ts.call("send", map[string]any{"instance_id": "inst_a", "resend": prepare}, nil)
	duplicate := stream.expect("session.prepare")
	if duplicate["message_id"] != original["message_id"] || payload(duplicate)["binding_id"] != payload(original)["binding_id"] {
		t.Errorf("duplicate %v of %v", duplicate, original)
	}
}

func TestEventsControlMakesEventsRequestsSilent(t *testing.T) {
	for _, mode := range []string{"hold_headers", "headers_only", "comments_only"} {
		t.Run(mode, func(t *testing.T) {
			ts := startServer(t)
			ts.call("events", map[string]any{"mode": mode}, nil)
			opened := make(chan *eventStream, 1)
			go func() {
				stream, _ := ts.open("tetral_ck_a", "inst_a", "cred_a", nil)
				opened <- stream
			}()
			waitFor(t, func() bool { return len(ts.state().Requests) == 1 })
			state := ts.state()
			if request := state.Requests[0]; request.Code != mode {
				t.Errorf("request %+v", request)
			}
			if len(state.Connections) != 0 || len(state.Events) != 0 {
				t.Errorf("a silent request allocated %d connections and sent %d events", len(state.Connections), len(state.Events))
			}
			if mode == "hold_headers" {
				ts.api.CloseClientConnections()
				if stream := <-opened; stream != nil {
					t.Fatal("the request got its headers")
				}
				return
			}
			stream := <-opened
			if stream == nil {
				t.Fatal("no headers")
			}
			if mode == "comments_only" {
				stream.comment()
			}
			ts.advance(time.Second)
			if mode == "comments_only" {
				stream.comment()
			}
			if next := ts.mustOpen("tetral_ck_a", "inst_a", "cred_a", nil); next.expect("connected") == nil {
				t.Fatal("the next request was silent too")
			}
			ts.api.CloseClientConnections()
			if rest := stream.waitEnded(); len(rest) != 0 {
				t.Errorf("the silent stream sent %v", rest)
			}
			if mode == "headers_only" && len(stream.comments) != 0 {
				t.Errorf("a headers-only stream sent %d comments", len(stream.comments))
			}
		})
	}

	t.Run("count", func(t *testing.T) {
		ts := startServer(t)
		ts.call("events", map[string]any{"mode": "headers_only", "count": 2}, nil)
		for range 2 {
			if stream, _ := ts.open("tetral_ck_a", "inst_a", "cred_a", nil); stream == nil {
				t.Fatal("no headers")
			}
		}
		if state := ts.state(); len(state.Requests) != 2 || state.Requests[1].Code != "headers_only" || len(state.Connections) != 0 {
			t.Fatalf("requests %+v with %d connections; want two silent requests", state.Requests, len(state.Connections))
		}
		if next := ts.mustOpen("tetral_ck_a", "inst_a", "cred_a", nil); next.expect("connected") == nil {
			t.Fatal("the third request was silent too")
		}
		ts.api.CloseClientConnections()
	})
}

func TestEventsControlDelaysConnectedFromTheControl(t *testing.T) {
	ts := startServer(t)
	if status, body := ts.callStatus("events", map[string]any{"mode": "delay_connected"}); status != http.StatusBadRequest {
		t.Errorf("delay_connected without delay_ms: %d %s", status, body)
	}
	ts.call("events", map[string]any{"mode": "delay_connected", "delay_ms": 2000}, nil)
	ts.advance(1999 * time.Millisecond)
	opened := make(chan *eventStream, 1)
	go func() {
		stream, _ := ts.open("tetral_ck_a", "inst_a", "cred_a", nil)
		opened <- stream
	}()
	waitFor(t, func() bool {
		ts.server.mu.Lock()
		defer ts.server.mu.Unlock()
		return len(ts.server.eventsModes) == 0
	})
	if state := ts.state(); len(state.Connections) != 0 || len(state.Requests) != 0 {
		t.Fatalf("%d connections and requests %+v before the delay ended", len(state.Connections), state.Requests)
	}
	ts.advance(time.Millisecond)
	select {
	case stream := <-opened:
		if stream == nil || stream.expect("connected") == nil {
			t.Fatal("no connected once the delay ended")
		}
	case <-time.After(waitLimit):
		t.Fatal("still waiting 2 seconds after the control")
	}
	if state := ts.state(); len(state.Requests) != 1 || state.Requests[0].Status != http.StatusOK || state.Requests[0].ConnectionID == "" {
		t.Errorf("requests %+v", state.Requests)
	}
	ts.api.CloseClientConnections()
}

func TestAnswerControlChangesTheNextAnswer(t *testing.T) {
	readyOn := func(t *testing.T) (*testServer, string) {
		ts := startServer(t)
		stream := ts.mustOpen("tetral_ck_a", "inst_a", "cred_a", nil)
		return ts, stream.expect("connected")["connection_id"].(string)
	}
	accepted := func(ts *testServer, connectionID string) bool { return ts.connection(connectionID).ReadyAccepted }

	t.Run("delay", func(t *testing.T) {
		ts, connectionID := readyOn(t)
		ts.call("answer", map[string]any{"endpoint": "ready", "action": "delay", "delay_ms": 3000}, nil)
		done := ts.postAsync("ready", "tetral_ck_a", readyBody(connectionID, "ready_1"))
		waitFor(t, func() bool { return len(ts.state().Held) == 1 })
		if !accepted(ts, connectionID) {
			t.Fatal("the delayed ready was not accepted")
		}
		ts.advance(2999 * time.Millisecond)
		if held := ts.state().Held; len(held) != 1 || held[0].MessageID != "ready_1" || !held[0].Until.Equal(testStart.Add(3*time.Second)) {
			t.Fatalf("held %+v after 2,999 ms; want the answer held until 3 seconds", held)
		}
		select {
		case answer := <-done:
			t.Fatalf("answered before the delay: %+v", answer)
		default:
		}
		ts.advance(time.Millisecond)
		if answer := receive(t, done); answer.status != http.StatusOK {
			t.Errorf("delayed answer %d %s", answer.status, answer.body)
		}
		if held := ts.state().Held; len(held) != 0 {
			t.Errorf("still held %+v", held)
		}
	})

	t.Run("drop", func(t *testing.T) {
		ts, connectionID := readyOn(t)
		ts.call("answer", map[string]any{"endpoint": "ready", "action": "drop"}, nil)
		if answer := ts.post("ready", "tetral_ck_a", readyBody(connectionID, "ready_1")); answer.err == nil {
			t.Fatalf("a dropped answer arrived: %d %s", answer.status, answer.body)
		}
		if !accepted(ts, connectionID) {
			t.Fatal("the dropped ready was not accepted")
		}
		if retry := ts.post("ready", "tetral_ck_a", readyBody(connectionID, "ready_1")); retry.status != http.StatusOK {
			t.Errorf("the retry: %d %s", retry.status, retry.body)
		}
	})

	t.Run("withhold", func(t *testing.T) {
		ts, connectionID := readyOn(t)
		ts.call("answer", map[string]any{"endpoint": "ready", "action": "withhold"}, nil)
		done := ts.postAsync("ready", "tetral_ck_a", readyBody(connectionID, "ready_1"))
		waitFor(t, func() bool { return len(ts.state().Held) == 1 })
		if !accepted(ts, connectionID) {
			t.Fatal("the withheld ready was not accepted")
		}
		ts.advance(time.Minute)
		if held := ts.state().Held; len(held) != 1 || held[0].MessageID != "ready_1" || !held[0].Until.IsZero() {
			t.Fatalf("held %+v after a minute; want the answer held until its release", held)
		}
		select {
		case answer := <-done:
			t.Fatalf("a withheld answer arrived: %+v", answer)
		default:
		}
		ts.call("answer", map[string]any{"action": "release"}, nil)
		if answer := receive(t, done); answer.status != http.StatusOK {
			t.Errorf("released answer %d %s", answer.status, answer.body)
		}
		if held := ts.state().Held; len(held) != 0 {
			t.Errorf("still held %+v", held)
		}
	})

	t.Run("wrong identity", func(t *testing.T) {
		ts, connectionID := readyOn(t)
		ts.call("answer", map[string]any{"endpoint": "ready", "action": "wrong_identity"}, nil)
		answer := ts.post("ready", "tetral_ck_a", readyBody(connectionID, "ready_1"))
		if answer.status != http.StatusOK || answer.member("message_id") != "other-ready_1" {
			t.Errorf("answer %d %s", answer.status, answer.body)
		}
	})

	t.Run("schema fail", func(t *testing.T) {
		ts, connectionID := readyOn(t)
		ts.call("answer", map[string]any{"endpoint": "ready", "action": "schema_fail", "body": map[string]any{
			"connection_id": connectionID, "message_id": "ready_1", "accepted": true, "valid_for_ms": 45001,
		}}, nil)
		answer := ts.post("ready", "tetral_ck_a", readyBody(connectionID, "ready_1"))
		if answer.status != http.StatusOK || answer.member("valid_for_ms") != float64(45001) {
			t.Errorf("answer %d %s", answer.status, answer.body)
		}
		ts.call("answer", map[string]any{"endpoint": "heartbeat", "action": "schema_fail"}, nil)
		if beat := ts.post("heartbeat", "tetral_ck_a", heartbeatBody(connectionID, "beat_1", "msg_x", 1)); string(beat.body) != "{}" {
			t.Errorf("default schema-failing body %s", beat.body)
		}
	})

	t.Run("error", func(t *testing.T) {
		ts, connectionID := readyOn(t)
		ts.call("answer", map[string]any{"endpoint": "ready", "message_id": "ready_1", "action": "error", "code": "rate_limited"}, nil)
		answer := ts.post("ready", "tetral_ck_a", readyBody(connectionID, "ready_1"))
		if answer.status != http.StatusTooManyRequests || answer.code() != "rate_limited" || accepted(ts, connectionID) {
			t.Errorf("answer %d %s, accepted %v", answer.status, answer.body, accepted(ts, connectionID))
		}
		if retry := ts.post("ready", "tetral_ck_a", readyBody(connectionID, "ready_1")); retry.status != http.StatusOK {
			t.Errorf("the next answer is changed too: %d %s", retry.status, retry.body)
		}
	})

	t.Run("count", func(t *testing.T) {
		ts, connectionID := readyOn(t)
		ts.call("answer", map[string]any{"endpoint": "ready", "action": "error", "code": "rate_limited", "count": 2}, nil)
		for attempt := range 2 {
			if answer := ts.post("ready", "tetral_ck_a", readyBody(connectionID, "ready_1")); answer.status != http.StatusTooManyRequests {
				t.Fatalf("request %d: %d %s; want the fault on each of the first two", attempt+1, answer.status, answer.body)
			}
		}
		if answer := ts.post("ready", "tetral_ck_a", readyBody(connectionID, "ready_1")); answer.status != http.StatusOK {
			t.Errorf("the third request: %d %s; want it answered as without the fault", answer.status, answer.body)
		}
	})

	t.Run("a fault for another message", func(t *testing.T) {
		ts, connectionID := readyOn(t)
		ts.call("answer", map[string]any{"endpoint": "ready", "message_id": "ready_other", "action": "error", "code": "rate_limited"}, nil)
		if answer := ts.post("ready", "tetral_ck_a", readyBody(connectionID, "ready_1")); answer.status != http.StatusOK || !accepted(ts, connectionID) {
			t.Errorf("answer %d %s; want the ready answered as without the fault", answer.status, answer.body)
		}
	})

	t.Run("error with a given status", func(t *testing.T) {
		ts, connectionID := readyOn(t)
		ts.call("answer", map[string]any{"endpoint": "ready", "action": "error", "code": "unavailable", "status": 503}, nil)
		answer := ts.post("ready", "tetral_ck_a", readyBody(connectionID, "ready_1"))
		if answer.status != http.StatusServiceUnavailable || answer.code() != "unavailable" || accepted(ts, connectionID) {
			t.Errorf("answer %d %s, accepted %v", answer.status, answer.body, accepted(ts, connectionID))
		}
		if status, body := ts.callStatus("answer", map[string]any{"endpoint": "ready", "action": "error", "code": "unavailable"}); status != http.StatusBadRequest {
			t.Errorf("a code outside the client error table without a status: %d %s", status, body)
		}
	})
}

func TestDispatchBeforeReadyAckControlSendsInstructionsBeforeTheAck(t *testing.T) {
	ts := startServer(t)
	stream := ts.mustOpen("tetral_ck_a", "inst_a", "cred_a", nil)
	connectionID := stream.expect("connected")["connection_id"].(string)
	instructions := []map[string]any{}
	for index := range StagingLimit + 1 {
		instructions = append(instructions, map[string]any{"type": "session.prepare", "payload": preparePayload("sess_"+twoDigits(index), "bind_"+twoDigits(index))})
	}
	ts.call("dispatch_before_ready_ack", map[string]any{"instance_id": "inst_a", "instructions": instructions}, nil)
	if kinds := ts.eventsOf(connectionID); !slices.Equal(kinds, []string{"connected"}) {
		t.Fatalf("events %v before any ready; the instructions wait for it", kinds)
	}
	ts.call("answer", map[string]any{"endpoint": "ready", "action": "withhold"}, nil)
	done := ts.postAsync("ready", "tetral_ck_a", readyBody(connectionID, "ready_1"))
	for range StagingLimit + 1 {
		stream.expect("session.prepare")
	}
	if held := ts.state().Held; len(held) != 1 || held[0].Endpoint != "ready" {
		t.Fatalf("held %+v; want the ready's ACK held while the instructions arrive", held)
	}
	ts.call("answer", map[string]any{"action": "release"}, nil)
	if answer := receive(t, done); answer.status != http.StatusOK {
		t.Errorf("ACK %d %s", answer.status, answer.body)
	}
	if issued := len(ts.connection(connectionID).Issued); issued != StagingLimit+1 {
		t.Errorf("issued %d; the early instructions pass the staging limit", issued)
	}
}

func TestBreakStreamControlKeepsTheLeaseForAResume(t *testing.T) {
	ts := startServer(t)
	stream, connectionID := ts.online()
	prepare := ts.prepare("sess_1", "bind_1")
	stream.expect("session.prepare")
	lease := ts.connection(connectionID).LeaseUntil
	// A break that granted the lease again would move it by this advance.
	ts.advance(2 * time.Second)
	ts.call("break_stream", map[string]any{"instance_id": "inst_a"}, nil)
	stream.waitEnded()
	if conn := ts.connection(connectionID); !conn.Live || conn.StreamOpen || !conn.LeaseUntil.Equal(lease) {
		t.Fatalf("after the break %+v", conn)
	}
	ts.advance(38 * time.Second)
	resumed := ts.mustOpen("tetral_ck_a", "inst_a", "cred_a", url.Values{"previous_connection_id": {connectionID}, "resume": {"true"}})
	if connected := resumed.expect("connected"); connected["connection_id"] != connectionID || payload(connected)["resumed"] != true {
		t.Fatalf("resume %v", connected)
	}
	beat := resumed.expect("heartbeat")
	if resent := resumed.expect("session.prepare"); resent["message_id"] != prepare {
		t.Errorf("resent %v", resent)
	}
	if conn := ts.connection(connectionID); !conn.LeaseUntil.Equal(lease) || conn.Issued[0].Sends != 2 {
		t.Errorf("the resume renewed the lease or did not resend once: %+v", conn)
	}
	seq := int64(payload(beat)["heartbeat_seq"].(float64))
	answer := ts.post("heartbeat", "tetral_ck_a", heartbeatBody(connectionID, "beat_1", beat["message_id"].(string), seq))
	if answer.status != http.StatusOK || !ts.connection(connectionID).LeaseUntil.After(lease) {
		t.Errorf("the heartbeat's ACK did not extend the lease: %d %s", answer.status, answer.body)
	}
}

func TestClockControlDrivesHeartbeatsLeasesAndStreamAge(t *testing.T) {
	ts := startServer(t, func(config *Config) { config.MaxStreamAgeMs = (40 * time.Second).Milliseconds() })
	stream, connectionID := ts.online()
	sent := func(kind string) int {
		count := 0
		for _, sentKind := range ts.eventsOf(connectionID) {
			if sentKind == kind {
				count++
			}
		}
		return count
	}
	for beat := 1; beat <= 2; beat++ {
		ts.advance(HeartbeatInterval - time.Millisecond)
		if heartbeats := sent("heartbeat"); heartbeats != beat-1 {
			t.Fatalf("%d heartbeats 1 ms before heartbeat %d is due", heartbeats, beat)
		}
		ts.advance(time.Millisecond)
		if heartbeats := sent("heartbeat"); heartbeats != beat {
			t.Fatalf("%d heartbeats when heartbeat %d is due", heartbeats, beat)
		}
		message := stream.expect("heartbeat")
		ts.post("heartbeat", "tetral_ck_a", heartbeatBody(connectionID, "beat_"+twoDigits(beat), message["message_id"].(string), int64(beat)))
	}
	ts.advance(10*time.Second - time.Millisecond)
	if disconnects := sent("disconnect"); disconnects != 0 {
		t.Fatal("a disconnect before the maximum age")
	}
	ts.advance(time.Millisecond)
	if rest := stream.waitEnded(); len(rest) != 1 || rest[0]["type"] != "disconnect" || payload(rest[0])["scope"] != "stream" {
		t.Fatalf("at the maximum age %v", rest)
	}
	// The second heartbeat's ACK, at 30 seconds, granted the lease until 75.
	ts.advance(35*time.Second - time.Millisecond)
	if !ts.connection(connectionID).Live {
		t.Fatal("the connection ended before its lease")
	}
	ts.advance(time.Millisecond)
	if ts.connection(connectionID).Live {
		t.Error("the connection outlived its lease")
	}
	if now := ts.state().Now; !now.Equal(testStart.Add(75 * time.Second)) {
		t.Errorf("now %v", now)
	}
}

func TestOneClockAdvanceDoesEverythingItMakesDueInTimeOrder(t *testing.T) {
	t.Run("heartbeats", func(t *testing.T) {
		ts := startServer(t)
		stream, connectionID := ts.online()
		ts.advance(2*HeartbeatInterval + 5*time.Second)
		stream.expect("heartbeat")
		second := stream.expect("heartbeat")
		ts.post("heartbeat", "tetral_ck_a", heartbeatBody(connectionID, "beat_2", second["message_id"].(string), 2))
		// The third heartbeat is due 15 seconds after the second was due,
		// not after the advance that sent it.
		ts.advance(HeartbeatInterval - 5*time.Second)
		if third := stream.expect("heartbeat"); payload(third)["heartbeat_seq"] != float64(3) {
			t.Errorf("third heartbeat %v", third)
		}
	})
	t.Run("maximum age before the lease end", func(t *testing.T) {
		ts := startServer(t, func(config *Config) { config.MaxStreamAgeMs = (40 * time.Second).Milliseconds() })
		_, connectionID := ts.online()
		ts.advance(50 * time.Second)
		kinds := ts.eventsOf(connectionID)
		if want := []string{"connected", "heartbeat", "heartbeat", "disconnect"}; !slices.Equal(kinds, want) {
			t.Errorf("events %v; want %v", kinds, want)
		}
		if ts.connection(connectionID).Live {
			t.Error("the connection outlived its lease")
		}
	})
}

func TestStateControlReportsWhatTheServerReceived(t *testing.T) {
	ts := startServer(t)
	_, connectionID := ts.online()
	ts.post("protocol-error", "tetral_ck_a", map[string]any{
		"protocol_version": 1, "connection_id": connectionID, "message_id": "error_1", "request_message_id": "msg_bad",
		"error": map[string]any{"code": "invalid_message", "message": "an envelope that is not JSON"},
	})
	ts.post("heartbeat", "tetral_ck_a", heartbeatBody(connectionID, "beat_1", "msg_x", 0))
	prepareID := ts.prepare("sess_1", "bind_1")
	state := ts.state()
	var endpoints []string
	for _, received := range state.Received {
		endpoints = append(endpoints, received.Endpoint+"/"+received.MessageID)
	}
	if len(endpoints) != 2 || endpoints[0] != "ready/ready_1" || endpoints[1] != "protocol-error/error_1" {
		t.Errorf("received %v", endpoints)
	}
	last := state.Requests[len(state.Requests)-1]
	if last.Endpoint != "heartbeat" || last.Status != http.StatusBadRequest || last.Code != "invalid_message" {
		t.Errorf("last request %+v", last)
	}
	if conn := state.Connections[0]; conn.ConnectionID != connectionID || !conn.ReadyAccepted || !conn.LeaseUntil.Equal(testStart.Add(GrantMax)) {
		t.Errorf("connection %+v", conn)
	}
	want := []BindingState{{BindingID: "bind_1", SessionID: "sess_1", PrepareMessageID: prepareID}}
	if bindings := state.Connections[0].Bindings; !slices.Equal(bindings, want) {
		t.Errorf("bindings %+v, want %+v", bindings, want)
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitLimit)
	defer cancel()
	for !condition() {
		select {
		case <-ctx.Done():
			t.Fatal(errors.New("the condition never held"))
		case <-time.After(time.Millisecond):
		}
	}
}

// twoDigits writes n, from 0 to 99, in two decimal digits.
func twoDigits(n int) string {
	return string(rune('0'+n/10)) + string(rune('0'+n%10))
}
