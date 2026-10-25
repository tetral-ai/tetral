package computerhub

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/enginetest"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
)

// renewal issues a stream-delivered credential with body for cli's online
// instance, checks the 202 answer, and reads its credential.renew from
// stream.
func (f *hubFixture) renewal(cli *enginetest.CLI, stream *testStream, key, body string) map[string]any {
	f.t.Helper()
	status, credential := f.issue(cli.InstanceID, key, body)
	if key, present := credential["connection_key"]; status != http.StatusAccepted || credential["state"] != "pending" || credential["in_use"] != false || !present || key != nil {
		f.t.Fatalf("stream issuance = %d %v; want 202 with a pending credential, not in use, connection_key null", status, credential)
	}
	event := stream.next()
	if payload, ok := event.Envelope.Payload.(protocol.CredentialRenewPayload); !ok || payload.CredentialID != credential["id"] {
		f.t.Fatalf("event %s; want credential.renew of %s", event.Data, credential["id"])
	}
	return credential
}

// fetch sends POST /v1/client/credential for credentialID on connectionID.
func (f *hubFixture) fetch(cli *enginetest.CLI, connectionID, messageID, credentialID string) enginetest.Response {
	f.t.Helper()
	response, err := cli.Post(f.ctx, "/v1/client/credential", protocol.Encode(protocol.CredentialRequest{ConnectionID: connectionID, MessageID: messageID, CredentialID: credentialID}))
	if err != nil {
		f.t.Fatal(err)
	}
	return response
}

// fetched fetches credentialID, checks that the credential stores the
// SHA-256 digest of the returned key, and returns a CLI that uses it.
func (f *hubFixture) fetched(cli *enginetest.CLI, connectionID, credentialID string) *enginetest.CLI {
	f.t.Helper()
	response := f.fetch(cli, connectionID, "msg_fetch_"+credentialID, credentialID)
	key, err := protocol.DecodeCredentialResponse(response.Body)
	if response.Status != http.StatusOK || err != nil || key.CredentialID != credentialID || response.Header.Get("Cache-Control") != "no-store" {
		f.t.Fatalf("fetch = %d %v %s; want 200 with the key and Cache-Control no-store", response.Status, response.Header, response.Body)
	}
	if expiresAt := f.credentialExpiry(credentialID); !parseTime(f.t, key.ExpiresAt).Equal(expiresAt) {
		f.t.Fatalf("fetched expires_at %s; the credential expires at %s", key.ExpiresAt, expiresAt)
	}
	digest := sha256.Sum256([]byte(key.ConnectionKey))
	if stored := f.credentialRow(credentialID)["key_digest"]; stored != `\x`+hex.EncodeToString(digest[:]) {
		f.t.Fatalf("stored key_digest %v; want the SHA-256 digest of the fetched key", stored)
	}
	return f.cli(cli.InstanceID, credentialID)
}

// credentialRow reads a credential row through the admin pool.
func (f *hubFixture) credentialRow(credentialID string) map[string]any {
	f.t.Helper()
	var data []byte
	if err := f.admin.QueryRowContext(f.ctx, `SELECT row_to_json(c) FROM instance_credentials c WHERE id=$1`, credentialID).Scan(&data); err != nil {
		f.t.Fatal(err)
	}
	var row map[string]any
	decodeJSON(f.t, data, &row)
	return row
}

// credentialState returns the state the credential list shows.
func (f *hubFixture) credentialState(instanceID, credentialID string) string {
	f.t.Helper()
	status, data := f.do(http.MethodGet, "/v1/instances/"+instanceID+"/credentials", nil, "")
	var list struct {
		Data []map[string]any `json:"data"`
	}
	decodeJSON(f.t, data, &list)
	for _, credential := range list.Data {
		if status == http.StatusOK && credential["id"] == credentialID {
			return credential["state"].(string)
		}
	}
	f.t.Fatalf("credential %s not listed (%d)", credentialID, status)
	return ""
}

func TestStreamIssuanceRenewsOnlyAnOnlineConnection(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	cli, stream := f.openOnline()
	notices := f.listen(instance.ConnectionNoticeChannel(hubTestPod))
	credential := f.renewal(cli, stream, "renew-online", `{}`)
	requireNoEvent(t, stream)
	if got := notices.within(300 * time.Millisecond); len(got) != 1 || got[0].notice.ConnectionID != stream.connectionID {
		t.Fatalf("notices = %+v; want the issuance's one notice of %s", got, stream.connectionID)
	}
	status, replayed := f.issue(cli.InstanceID, "renew-online", `{}`)
	if key, present := replayed["connection_key"]; status != http.StatusAccepted || replayed["id"] != credential["id"] || !present || key != nil {
		t.Fatalf("replay = %d %v; want 202 with %s and connection_key null", status, replayed, credential["id"])
	}
	requireNoEvent(t, stream)

	registering := f.userInstance(time.Time{})
	f.open(registering)
	for name, instanceID := range map[string]string{"no connection": f.userInstance(time.Time{}).InstanceID, "ready not accepted": registering.InstanceID} {
		before := f.rowCounts()
		status, data := f.do(http.MethodPost, "/v1/instances/"+instanceID+"/credentials", []string{"renew-" + name}, `{"delivery":"stream"}`)
		requireAPIError(t, status, data, http.StatusConflict, "instance_offline_error", "")
		if after := f.rowCounts(); after != before {
			t.Fatalf("%s: rows %v after the refusal; want %v", name, after, before)
		}
	}
}

func TestRenewalOfAnEndedConnectionStaysPendingAndIsNotSentAgain(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	cli, stream := f.openOnline()
	pending := f.renewal(cli, stream, "renew-ended", `{}`)["id"].(string)
	if _, err := f.admin.ExecContext(f.ctx, `UPDATE instance_connections SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, stream.connectionID); err != nil {
		t.Fatal(err)
	}
	resume := false
	next := f.openClientStream(f.url, cli, cli.EventsQuery(stream.connectionID, &resume), false)
	f.acceptReady(cli, next, "ready_next")
	// A pass after the accepted ready writes the instruction, and nothing
	// before it.
	instruction := f.issueInstruction(f.ctx, next.connectionID, "sess_after_end")
	f.requireInstructions(next, instruction)
	requireNoEvent(t, next)
	if state := f.credentialState(cli.InstanceID, pending); state != "pending" {
		t.Fatalf("renewal of the ended connection lists %s; want pending", state)
	}
	if !f.credentialInUse(cli.InstanceID, cli.CredentialID) {
		t.Fatal("the old credential does not hold the new connection")
	}
	if status, _ := f.do(http.MethodDelete, "/v1/instances/"+cli.InstanceID+"/credentials/"+pending, nil, ""); status != http.StatusNoContent {
		t.Fatalf("revoke = %d", status)
	}
	if state := f.credentialState(cli.InstanceID, pending); state != "revoked" {
		t.Fatalf("revoked renewal lists %s", state)
	}
}

func TestFetchesOfAnyOtherCredentialAnswerNotFound(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	cli, stream := f.openOnline()
	pending := f.renewal(cli, stream, "renew-a", `{}`)["id"].(string)
	other, otherStream := f.openOnline()
	foreign := f.renewal(other, otherStream, "renew-b", `{}`)["id"].(string)
	third, thirdStream := f.openOnline()
	revoked := f.renewal(third, thirdStream, "renew-c-revoked", `{}`)["id"].(string)
	if status, _ := f.do(http.MethodDelete, "/v1/instances/"+third.InstanceID+"/credentials/"+revoked, nil, ""); status != http.StatusNoContent {
		t.Fatalf("revoke = %d", status)
	}
	expired := f.renewal(third, thirdStream, "renew-c-expired", `{}`)["id"].(string)
	renewed, renewedStream := f.openOnline()
	activated := f.renewal(renewed, renewedStream, "renew-d", `{}`)["id"].(string)
	renewedCLI := f.fetched(renewed, renewedStream.connectionID, activated)
	f.resume(f.url, renewedCLI, renewedStream.connectionID)
	if _, err := f.admin.ExecContext(f.ctx, `UPDATE instance_credentials SET issued_at=clock_timestamp()-interval '2 hours',expires_at=clock_timestamp()-interval '1 hour' WHERE id=$1`, expired); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		fetcher                    *enginetest.CLI
		connectionID, credentialID string
	}{
		"unknown":           {cli, stream.connectionID, "ccred_" + strings.Repeat("0", 16)},
		"another instance":  {cli, stream.connectionID, foreign},
		"active":            {cli, stream.connectionID, cli.CredentialID},
		"activated renewal": {renewedCLI, renewedStream.connectionID, activated},
		"revoked":           {third, thirdStream.connectionID, revoked},
		"expired":           {third, thirdStream.connectionID, expired},
	} {
		response := f.fetch(tc.fetcher, tc.connectionID, "msg_fetch_"+name, tc.credentialID)
		requireClientError(t, response, nil, protocol.ErrorNotFound)
		if tc.credentialID != activated && f.count(`SELECT count(*) FROM instance_credentials WHERE id=$1 AND fetched_at IS NOT NULL`, tc.credentialID) != 0 {
			t.Fatalf("%s: the refused fetch stored a key", name)
		}
	}
	if f.count(`SELECT count(*) FROM connection_upstream_messages WHERE connection_id<>$1`, renewedStream.connectionID) != 0 {
		t.Fatal("a refused fetch wrote a replay record")
	}

	// The CLI never reads the first answer; its retry is checked again.
	if first := f.fetch(cli, stream.connectionID, "msg_fetch_dropped", pending); first.Status != http.StatusOK {
		t.Fatalf("first fetch = %d %s", first.Status, first.Body)
	}
	requireClientError(t, f.fetch(cli, stream.connectionID, "msg_fetch_dropped", pending), nil, protocol.ErrorNotFound)
	requireClientError(t, f.fetch(cli, stream.connectionID, "msg_fetch_dropped", foreign), nil, protocol.ErrorMessageConflict)
	if state := f.credentialState(cli.InstanceID, pending); state != "pending" {
		t.Fatalf("fetched renewal lists %s; want pending", state)
	}
	for _, instance := range []struct {
		cli    *enginetest.CLI
		stream *testStream
	}{{cli, stream}, {third, thirdStream}} {
		requireConnectionObject(t, f.connectionState(instance.cli.InstanceID), instance.stream.connectionID, instance.cli.CredentialID, "online")
	}
}

func TestResumeWithTheFetchedCredentialActivatesItAndRevokesTheOld(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	cli, stream := f.openOnline()
	expiresAt := f.dbNow().Add(20 * time.Second).UTC().Format(time.RFC3339Nano)
	pending := f.renewal(cli, stream, "renew-activate", `{"expires_at":"`+expiresAt+`"}`)["id"].(string)
	renewed := f.fetched(cli, stream.connectionID, pending)

	before := [3]map[string]any{f.credentialRow(cli.CredentialID), f.credentialRow(pending), f.connectionRow(stream.connectionID)}
	response, err := renewed.Heartbeat(f.ctx, protocol.Heartbeat{ConnectionID: stream.connectionID, MessageID: "msg_pending_beat", RequestMessageID: instance.HeartbeatMessageID(stream.connectionID, 1), HeartbeatSeq: 1})
	requireClientError(t, response, err, protocol.ErrorCredentialInvalid)
	if after := [3]map[string]any{f.credentialRow(cli.CredentialID), f.credentialRow(pending), f.connectionRow(stream.connectionID)}; string(mustJSON(t, after)) != string(mustJSON(t, before)) {
		t.Fatalf("the refused heartbeat changed rows:\n%v\n%v", before, after)
	}

	resumed := f.resume(f.url, renewed, stream.connectionID)
	old, activated := f.credentialRow(cli.CredentialID), f.credentialRow(pending)
	if activated["state"] != "active" || old["state"] != "revoked" || old["revocation_cause"] != "pending_activation" || old["revoked_at"] != activated["used_at"] {
		t.Fatalf("after the resume: old %v, new %v; want the new active and the old revoked by its activation at once", old, activated)
	}
	if f.connectionRow(stream.connectionID)["credential_id"] != pending || !f.credentialInUse(cli.InstanceID, pending) || f.credentialInUse(cli.InstanceID, cli.CredentialID) {
		t.Fatal("in_use did not move to the activated credential")
	}
	report := resumed.heartbeat("msg_after_activation")
	grant, err := renewed.Heartbeat(f.ctx, report)
	if err != nil {
		t.Fatal(err)
	}
	ack, err := grant.Grant()
	lease := parseTime(t, f.connectionRow(stream.connectionID)["lease_expires_at"])
	if err != nil || ack.ValidForMS > 20_000 || lease.After(f.credentialExpiry(pending)) {
		t.Fatalf("grant after the activation: %+v %v, lease %v; want it capped by %s", ack, err, lease, expiresAt)
	}
}

func TestOnlyUsesOfACredentialAnActivationRevokedAreRecorded(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	select {
	case <-f.events.listening:
	case <-f.ctx.Done():
		t.Fatal("the instance event listener never listened")
	}
	events := streamedEventsOf(t, f.openStream(f.principal))
	cli, stream := f.openOnline()
	pending := f.renewal(cli, stream, "renew-reuse", `{}`)["id"].(string)
	f.resume(f.url, f.fetched(cli, stream.connectionID, pending), stream.connectionID)

	_, response, err := cli.OpenEvents(f.ctx, cli.EventsQuery(stream.connectionID, nil))
	requireClientError(t, response, err, protocol.ErrorCredentialRevoked)
	reuse, _ := f.instanceRead(cli.InstanceID)["last_credential_reuse"].(map[string]any)
	if reuse == nil || reuse["credential_id"] != cli.CredentialID || reuse["detected_at"] == nil {
		t.Fatalf("last_credential_reuse = %v; want the old credential", reuse)
	}
	if event := events.next(t); event.name != "credential_reused" || event.instanceID != cli.InstanceID {
		t.Fatalf("instance event %+v; want credential_reused of %s", event, cli.InstanceID)
	}
	events.none(t)

	deleted, deletedStream := f.openOnline()
	if status, _ := f.do(http.MethodDelete, "/v1/instances/"+deleted.InstanceID+"/credentials/"+deleted.CredentialID, nil, ""); status != http.StatusNoContent {
		t.Fatalf("revoke = %d", status)
	}
	response2, err := deleted.Heartbeat(f.ctx, protocol.Heartbeat{ConnectionID: deletedStream.connectionID, MessageID: "msg_deleted_beat", RequestMessageID: instance.HeartbeatMessageID(deletedStream.connectionID, 1), HeartbeatSeq: 1})
	requireClientError(t, response2, err, protocol.ErrorCredentialRevoked)
	if reuse := f.instanceRead(deleted.InstanceID)["last_credential_reuse"]; reuse != nil {
		t.Fatalf("a use of a deleted credential recorded %v", reuse)
	}
	events.none(t)
}

// TestEachUseOfACredentialAnActivationRevokedIsRecorded sends two uses of a
// credential an activation revoked at once, and one use that took its first
// read before the activation and its locks after: each answers 401
// credential_revoked and sends its own credential_reused.
func TestEachUseOfACredentialAnActivationRevokedIsRecorded(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	select {
	case <-f.events.listening:
	case <-f.ctx.Done():
		t.Fatal("the instance event listener never listened")
	}
	events := streamedEventsOf(t, f.openStream(f.principal))
	use := func(cli *enginetest.CLI, connectionID string) <-chan enginetest.Response {
		done := make(chan enginetest.Response, 1)
		go func() {
			response, err := cli.Heartbeat(f.ctx, protocol.Heartbeat{ConnectionID: connectionID, MessageID: id.New("msg_"),
				RequestMessageID: instance.HeartbeatMessageID(connectionID, 1), HeartbeatSeq: 1})
			if err != nil {
				response.Status = 0
			}
			done <- response
		}()
		return done
	}
	requireRevoked := func(response enginetest.Response) {
		t.Helper()
		if body, err := response.Error(); err != nil || response.Status != http.StatusUnauthorized || body.Code != protocol.ErrorCredentialRevoked {
			t.Fatalf("use = %d %s; want 401 credential_revoked", response.Status, response.Body)
		}
	}
	requireReuses := func(cli *enginetest.CLI, n int) {
		t.Helper()
		for range n {
			if event := events.next(t); event.name != "credential_reused" || event.instanceID != cli.InstanceID {
				t.Fatalf("instance event %+v; want credential_reused of %s", event, cli.InstanceID)
			}
		}
		events.none(t)
		if reuse, _ := f.instanceRead(cli.InstanceID)["last_credential_reuse"].(map[string]any); reuse == nil || reuse["credential_id"] != cli.CredentialID {
			t.Fatalf("last_credential_reuse = %v; want %s", reuse, cli.CredentialID)
		}
	}

	t.Run("two uses at once", func(t *testing.T) {
		cli, stream := f.openOnline()
		pending := f.renewal(cli, stream, "renew-two-uses", `{}`)["id"].(string)
		f.resume(f.url, f.fetched(cli, stream.connectionID, pending), stream.connectionID)
		pause := f.pauseOn("UPDATE instances SET last_credential_reuse")
		release := sync.OnceFunc(func() { close(pause.release) })
		defer release()
		first := use(cli, stream.connectionID)
		<-pause.reached
		second := use(cli, stream.connectionID)
		f.awaitLockWait()
		release()
		requireRevoked(<-first)
		requireRevoked(<-second)
		requireReuses(cli, 2)
	})

	t.Run("a use the activation overtook", func(t *testing.T) {
		cli, stream := f.openOnline()
		pending := f.renewal(cli, stream, "renew-overtaken", `{}`)["id"].(string)
		renewed := f.fetched(cli, stream.connectionID, pending)
		pause := f.pauseOn("FROM instances WHERE workspace_id=$1 AND id=$2 FOR SHARE")
		release := sync.OnceFunc(func() { close(pause.release) })
		defer release()
		overtaken := use(cli, stream.connectionID)
		<-pause.reached
		f.resume(f.url, renewed, stream.connectionID)
		release()
		requireRevoked(<-overtaken)
		requireReuses(cli, 1)
	})
}

// streamedEvent is one event of the instance stream.
type streamedEvent struct {
	name, instanceID string
}

// streamedEvents reads an instance stream's events as they come.
type streamedEvents chan streamedEvent

func streamedEventsOf(t *testing.T, stream *bufio.Reader) streamedEvents {
	events := make(streamedEvents, 16)
	go func() {
		for {
			name, data := readServerSentEventOrEnd(stream)
			if name == "" {
				close(events)
				return
			}
			var object struct {
				ID string `json:"id"`
			}
			_ = json.Unmarshal([]byte(data), &object)
			events <- streamedEvent{name: name, instanceID: object.ID}
		}
	}()
	return events
}

func (e streamedEvents) next(t *testing.T) streamedEvent {
	t.Helper()
	select {
	case event := <-e:
		return event
	case <-time.After(10 * time.Second):
		t.Fatal("no instance event within 10 seconds")
	}
	return streamedEvent{}
}

func (e streamedEvents) none(t *testing.T) {
	t.Helper()
	select {
	case event := <-e:
		t.Fatalf("unexpected instance event %+v", event)
	case <-time.After(300 * time.Millisecond):
	}
}

// readServerSentEventOrEnd returns the next event's name and data, or an
// empty name once the stream ends.
func readServerSentEventOrEnd(stream *bufio.Reader) (string, string) {
	var event, data string
	for {
		line, err := stream.ReadString('\n')
		if err != nil {
			return "", ""
		}
		line = strings.TrimSuffix(line, "\n")
		switch {
		case line == "" && event != "":
			return event, data
		case strings.HasPrefix(line, "event: "):
			event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			data = strings.TrimPrefix(line, "data: ")
		}
	}
}
