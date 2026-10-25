package reference

import (
	"net/http"
	"net/url"
	"slices"
	"testing"
	"time"
)

func TestTheServerRefusesWhatEngineRefuses(t *testing.T) {
	credentialRows := []struct {
		name, key, credentialID, code string
	}{
		{"an unknown key", "tetral_ck_unknown", "cred_a", "credential_invalid"},
		{"another instance's key", "tetral_ck_b", "cred_b", "credential_invalid"},
		{"another credential's key of the instance", "tetral_ck_a", "cred_revoked", "credential_invalid"},
		{"a revoked credential", "tetral_ck_revoked", "cred_revoked", "credential_revoked"},
		{"an expired credential", "tetral_ck_expired", "cred_expired", "credential_expired"},
	}
	for _, row := range credentialRows {
		t.Run(row.name, func(t *testing.T) {
			ts := startServer(t)
			stream, refused := ts.open(row.key, "inst_a", row.credentialID, nil)
			if stream != nil || refused.status != http.StatusUnauthorized || refused.code() != row.code {
				t.Fatalf("got %d %s; want 401 %s", refused.status, refused.body, row.code)
			}
			if len(ts.state().Connections) != 0 {
				t.Fatal("a refused events request allocated a connection")
			}
		})
	}

	t.Run("reports that answer no instruction of the connection", func(t *testing.T) {
		ts := startServer(t)
		stream, connectionID := ts.online()
		prepare := ts.prepare("sess_1", "bind_1")
		stream.expect("session.prepare")
		rows := []struct {
			name   string
			report map[string]any
		}{
			{"an unissued instruction", preparedReport(connectionID, "report_1", "msg_unissued", "sess_1", "bind_1")},
			{"another binding", preparedReport(connectionID, "report_2", prepare, "sess_1", "bind_other")},
		}
		for _, row := range rows {
			if answer := ts.post("session-status", "tetral_ck_a", row.report); answer.status != http.StatusConflict || answer.code() != "unknown_instruction" {
				t.Errorf("%s: %d %s; want 409 unknown_instruction", row.name, answer.status, answer.body)
			}
		}
		if issued := ts.connection(connectionID).Issued[0]; issued.Reported {
			t.Error("a refused report was stored")
		}
	})

	t.Run("a report on an old connection", func(t *testing.T) {
		ts := startServer(t)
		_, old := ts.online()
		prepare := ts.prepare("sess_1", "bind_1")
		ts.call("break_stream", map[string]any{"instance_id": "inst_a"}, nil)
		ts.advance(GrantMax)
		fresh := ts.mustOpen("tetral_ck_a", "inst_a", "cred_a", nil)
		if current := fresh.expect("connected")["connection_id"]; current == old {
			t.Fatal("the expired connection resumed")
		}
		answer := ts.post("session-status", "tetral_ck_a", preparedReport(old, "report_1", prepare, "sess_1", "bind_1"))
		if answer.status != http.StatusConflict || answer.code() != "stale_connection" {
			t.Fatalf("got %d %s; want 409 stale_connection", answer.status, answer.body)
		}
	})

	t.Run("readies and heartbeats out of turn", func(t *testing.T) {
		ts := startServer(t)
		stream := ts.mustOpen("tetral_ck_a", "inst_a", "cred_a", nil)
		connectionID := stream.expect("connected")["connection_id"].(string)
		early := ts.post("heartbeat", "tetral_ck_a", heartbeatBody(connectionID, "beat_0", "msg_x", 1))
		if early.status != http.StatusBadRequest || early.code() != "invalid_message" {
			t.Errorf("a heartbeat before the ready: %d %s; want 400 invalid_message", early.status, early.body)
		}
		if answer := ts.post("ready", "tetral_ck_a", readyBody(connectionID, "ready_1")); answer.status != http.StatusOK {
			t.Fatalf("ready: %d %s", answer.status, answer.body)
		}
		var beats []map[string]any
		for index := range 2 {
			ts.advance(HeartbeatInterval)
			beat := stream.expect("heartbeat")
			beats = append(beats, beat)
			seq := int64(payload(beat)["heartbeat_seq"].(float64))
			accepted := ts.post("heartbeat", "tetral_ck_a", heartbeatBody(connectionID, "beat_"+twoDigits(index+1), beat["message_id"].(string), seq))
			if accepted.status != http.StatusOK {
				t.Fatalf("heartbeat %d: %d %s", seq, accepted.status, accepted.body)
			}
		}
		ts.advance(time.Second)
		lease := ts.connection(connectionID).LeaseUntil
		rows := []struct {
			name string
			path string
			body map[string]any
		}{
			{"a second ready under a new message_id", "ready", readyBody(connectionID, "ready_2")},
			{"a heartbeat whose sequence equals the last accepted", "heartbeat",
				heartbeatBody(connectionID, "beat_3", beats[1]["message_id"].(string), 2)},
			{"a heartbeat whose sequence is below the last accepted", "heartbeat",
				heartbeatBody(connectionID, "beat_4", beats[0]["message_id"].(string), 1)},
		}
		for _, row := range rows {
			answer := ts.post(row.path, "tetral_ck_a", row.body)
			if answer.status != http.StatusBadRequest || answer.code() != "invalid_message" {
				t.Errorf("%s: %d %s; want 400 invalid_message", row.name, answer.status, answer.body)
			}
		}
		state := ts.state()
		for _, received := range state.Received {
			if slices.Contains([]string{"beat_0", "ready_2", "beat_3", "beat_4"}, received.MessageID) {
				t.Errorf("refused message %s was recorded", received.MessageID)
			}
		}
		if after := ts.connection(connectionID).LeaseUntil; !after.Equal(lease) {
			t.Errorf("a refused message moved the lease from %v to %v", lease, after)
		}
	})

	t.Run("a second connection while the lease is live", func(t *testing.T) {
		ts := startServer(t)
		_, connectionID := ts.online()
		conflict, refused := ts.open("tetral_ck_a", "inst_a", "cred_a", nil)
		if conflict != nil || refused.status != http.StatusConflict || refused.code() != "connection_conflict" {
			t.Errorf("no predecessor: %d %s; want 409 connection_conflict", refused.status, refused.body)
		}
		ts.advance(10 * time.Second)
		fresh, refused := ts.open("tetral_ck_a", "inst_a", "cred_a", url.Values{"previous_connection_id": {connectionID}, "resume": {"false"}})
		if fresh != nil || refused.status != http.StatusConflict || refused.code() != "previous_connection_active" {
			t.Fatalf("a fresh start after the live connection: %d %s; want 409 previous_connection_active", refused.status, refused.body)
		}
		if retry := refused.member("retry_after_ms"); retry != float64((GrantMax - 10*time.Second).Milliseconds()) {
			t.Errorf("retry_after_ms %v; want the remaining lease", retry)
		}
		if !ts.connection(connectionID).Live {
			t.Error("a refused connection attempt replaced the live connection")
		}
	})
}

func TestARetryGetsTheOriginalAnswerAndOtherContentConflicts(t *testing.T) {
	ts := startServer(t)
	stream := ts.mustOpen("tetral_ck_a", "inst_a", "cred_a", nil)
	connectionID := stream.expect("connected")["connection_id"].(string)
	first := ts.post("ready", "tetral_ck_a", readyBody(connectionID, "ready_1"))
	ts.advance(5 * time.Second)
	lease := ts.connection(connectionID).LeaseUntil
	retry := ts.post("ready", "tetral_ck_a", readyBody(connectionID, "ready_1"))
	if retry.status != http.StatusOK || string(retry.body) != string(first.body) {
		t.Errorf("retry: %d %s; want the original %s", retry.status, retry.body, first.body)
	}
	if after := ts.connection(connectionID).LeaseUntil; !after.Equal(lease) {
		t.Error("a retry renewed the lease")
	}
	changed := readyBody(connectionID, "ready_1")
	changed["cli_version"] = "1.0.1"
	if conflict := ts.post("ready", "tetral_ck_a", changed); conflict.status != http.StatusConflict || conflict.code() != "message_conflict" {
		t.Errorf("other content: %d %s; want 409 message_conflict", conflict.status, conflict.body)
	}
}

func TestAReadyBelowTheMinimumVersionIsRefused(t *testing.T) {
	ts := startServer(t, func(config *Config) { config.MinCLIVersion = "1.2.0" })
	for _, row := range []struct {
		version string
		status  int
	}{{"1.1.9", http.StatusForbidden}, {"1.2.0-rc.1", http.StatusForbidden}, {"1.2.0", http.StatusOK}, {"1.10.0", http.StatusOK}} {
		stream := ts.mustOpen("tetral_ck_a", "inst_a", "cred_a", nil)
		connectionID := stream.expect("connected")["connection_id"].(string)
		body := readyBody(connectionID, "ready_1")
		body["cli_version"] = row.version
		answer := ts.post("ready", "tetral_ck_a", body)
		if answer.status != row.status || (row.status == http.StatusForbidden && answer.code() != "cli_upgrade_required") {
			t.Errorf("%s: %d %s; want %d", row.version, answer.status, answer.body, row.status)
		}
		ts.call("break_stream", map[string]any{"instance_id": "inst_a"}, nil)
		ts.advance(GrantMax)
	}
}

func TestAGrantIsCappedAtTheCredentialsExpiry(t *testing.T) {
	ts := startServer(t, func(config *Config) {
		config.Credentials[0].ExpiresAt = testStart.Add(10 * time.Second)
	})
	stream := ts.mustOpen("tetral_ck_a", "inst_a", "cred_a", nil)
	connectionID := stream.expect("connected")["connection_id"].(string)
	answer := ts.post("ready", "tetral_ck_a", readyBody(connectionID, "ready_1"))
	if answer.status != http.StatusOK || answer.member("valid_for_ms") != float64(10_000) {
		t.Fatalf("ready: %d %s; want valid_for_ms 10000", answer.status, answer.body)
	}
	ts.advance(10 * time.Second)
	if expired := ts.post("protocol-error", "tetral_ck_a", map[string]any{
		"protocol_version": 1, "connection_id": connectionID, "message_id": "error_1",
		"error": map[string]any{"code": "invalid_message", "message": "x"},
	}); expired.status != http.StatusUnauthorized || expired.code() != "credential_expired" {
		t.Errorf("after the expiry: %d %s; want 401 credential_expired", expired.status, expired.body)
	}
}

func TestAGitTicketNeedsAnIssuedCloudPrepareAndEachFetchIssuesANewOne(t *testing.T) {
	ts := startServer(t)
	stream := ts.mustOpen("tetral_ck_a", "inst_a", "cred_a", nil)
	connectionID := stream.expect("connected")["connection_id"].(string)
	cloud := preparePayload("sess_2", "bind_cloud")
	cloud["create_cwd"] = true
	cloud["repositories"] = []any{map[string]any{
		"url": "https://github.com/acme/backend", "checkout": nil, "path": "/home/user/project/backend",
		"git_identity": map[string]any{"name": "n", "email": "e@example.com"},
	}}
	ts.call("prepare", map[string]any{"instance_id": "inst_a", "payload": cloud}, nil)
	fetch := func(bindingID string) reply {
		return ts.post("git-ticket", "tetral_ck_a", map[string]any{
			"protocol_version": 1, "connection_id": connectionID, "message_id": "ticket_1", "binding_id": bindingID,
		})
	}
	if refused := fetch("bind_cloud"); refused.status != http.StatusConflict || refused.code() != "unknown_instruction" {
		t.Errorf("a prepare queued before the ready: %d %s; want 409 unknown_instruction", refused.status, refused.body)
	}
	if answer := ts.post("ready", "tetral_ck_a", readyBody(connectionID, "ready_1")); answer.status != http.StatusOK {
		t.Fatalf("ready: %d %s", answer.status, answer.body)
	}
	stream.expect("session.prepare")
	plain := ts.prepare("sess_1", "bind_plain")
	stream.expect("session.prepare")
	if refused := fetch("bind_plain"); refused.status != http.StatusConflict || refused.code() != "unknown_instruction" {
		t.Errorf("a binding without repositories (%s): %d %s; want 409 unknown_instruction", plain, refused.status, refused.body)
	}
	first, second := fetch("bind_cloud"), fetch("bind_cloud")
	for _, answer := range []reply{first, second} {
		if answer.status != http.StatusOK || answer.header.Get("Cache-Control") != "no-store" || answer.member("git_proxy_url") != DefaultGitProxyURL {
			t.Fatalf("fetch: %d %v %s", answer.status, answer.header, answer.body)
		}
	}
	if first.member("ticket") == second.member("ticket") {
		t.Error("a retry with the same message_id returned the same ticket")
	}
	if tickets := ts.state().Tickets["inst_a/sess_2"]; len(tickets) != 2 {
		t.Errorf("tickets %v; want two, the first revoked by the second", tickets)
	}
}

func TestARenewalIsFetchedOnceAndCommittedByAResumeOrANewConnection(t *testing.T) {
	for _, commit := range []string{"resume", "new connection"} {
		t.Run(commit, func(t *testing.T) { commitsARenewal(t, commit) })
	}
}

func commitsARenewal(t *testing.T, commit string) {
	ts := startServer(t)
	stream, connectionID := ts.online()
	ts.call("credential", map[string]any{"action": "add", "renew": true, "credential": Credential{
		InstanceID: "inst_a", CredentialID: "cred_new", ExpiresAt: testStart.Add(2 * time.Hour), State: "pending",
	}}, nil)
	if offered := stream.expect("credential.renew"); payload(offered)["credential_id"] != "cred_new" {
		t.Fatalf("offer %v", offered)
	}
	fetch := map[string]any{"protocol_version": 1, "connection_id": connectionID, "message_id": "fetch_1", "credential_id": "cred_new"}
	fetched := ts.post("credential", "tetral_ck_a", fetch)
	key, _ := fetched.member("connection_key").(string)
	if fetched.status != http.StatusOK || fetched.header.Get("Cache-Control") != "no-store" || key == "" {
		t.Fatalf("fetch: %d %v %s", fetched.status, fetched.header, fetched.body)
	}
	if again := ts.post("credential", "tetral_ck_a", fetch); again.status != http.StatusNotFound || again.code() != "not_found" {
		t.Errorf("a retry of the fetch: %d %s; want 404 not_found", again.status, again.body)
	}
	for _, unknown := range []string{"cred_missing", "cred_b", "cred_a", "cred_expired", "cred_revoked"} {
		fetch["credential_id"], fetch["message_id"] = unknown, "fetch_"+unknown
		if refused := ts.post("credential", "tetral_ck_a", fetch); refused.status != http.StatusNotFound {
			t.Errorf("fetching %s: %d %s; want 404 not_found", unknown, refused.status, refused.body)
		}
	}
	ts.call("break_stream", map[string]any{"instance_id": "inst_a"}, nil)
	committed := connectionID
	if commit == "resume" {
		resumed := ts.mustOpen(key, "inst_a", "cred_new", url.Values{"previous_connection_id": {connectionID}, "resume": {"true"}})
		if connected := resumed.expect("connected"); connected["connection_id"] != connectionID || payload(connected)["resumed"] != true {
			t.Fatalf("resume %v", connected)
		}
	} else {
		ts.advance(GrantMax)
		fresh := ts.mustOpen(key, "inst_a", "cred_new", nil)
		connected := fresh.expect("connected")
		committed, _ = connected["connection_id"].(string)
		if committed == connectionID || payload(connected)["resumed"] == true {
			t.Fatalf("new connection %v", connected)
		}
	}
	if old := ts.post("protocol-error", "tetral_ck_a", map[string]any{
		"protocol_version": 1, "connection_id": committed, "message_id": "error_1",
		"error": map[string]any{"code": "invalid_message", "message": "x"},
	}); old.status != http.StatusUnauthorized || old.code() != "credential_revoked" {
		t.Errorf("the old credential: %d %s; want 401 credential_revoked", old.status, old.body)
	}
	state := ts.state()
	if conn := ts.connection(committed); conn.CredentialID != "cred_new" {
		t.Errorf("the connection's credential is %s", conn.CredentialID)
	}
	for _, cred := range state.Credentials {
		if cred.CredentialID == "cred_a" && (cred.State != "revoked" || cred.Reuses != 1) {
			t.Errorf("old credential %+v; want revoked with one reuse", cred)
		}
	}
}
