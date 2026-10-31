package joint

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/enginetest"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/gitticket"
	"github.com/tetral-ai/tetral/internal/protocol"
)

// TestKilledHubPodsCLIsResumeOnOtherPods runs two CLIs against two
// computer-hub pods, one stream on each, and kills the pod that holds the
// first CLI's stream, with no drain. The first CLI resumes the same
// connection on the other pod before its lease expires; its connection stays
// online throughout and accepts a heartbeat after the resume. The second
// CLI keeps its stream on the other pod.
func TestKilledHubPodsCLIsResumeOnOtherPods(t *testing.T) {
	in := requireInputs(t, needs{})
	record := newAcceptanceRecord(t, in, platformOf(in))
	record.boundary("no task runs and no instruction waits when the pod dies: the test creates no Session, and no Engine path routes a tool call to a computer")
	e := startEngine(t, record, engineOptions{hubPods: 2})
	controller := e.controller()
	set := newMachines(t, in, needs{}, e, record)
	ctx := t.Context()

	type cli struct {
		m          *machine
		instanceID string
		stream     eventsRequest
		streams    int
	}
	var clis []cli
	for _, name := range []string{"first", "second"} {
		if len(clis) == 1 {
			// The second CLI's stream goes to the pod the first does not use.
			e.edge.setDown(clis[0].stream.backend, true)
		}
		registered := controller.createUserInstance(ctx, name)
		m := set.add(name, name+"-app")
		m.configure(bootstrap(e.serverURL, registered.instance.ID, registered.credential))
		m.serve()
		requireOnline(t, controller, m, registered.instance.ID, registered.credential.ID)
		requests := e.edge.eventsRequests(registered.instance.ID)
		clis = append(clis, cli{m: m, instanceID: registered.instance.ID, stream: requests[len(requests)-1], streams: len(requests)})
	}
	e.edge.setDown(clis[0].stream.backend, false)
	if clis[0].stream.backend == clis[1].stream.backend {
		t.Fatalf("both streams on %s; want one on each pod", clis[0].stream.backend)
	}

	killed := clis[0].stream.backend
	leases := map[string]connectionObject{}
	for _, c := range clis {
		leases[c.instanceID] = *controller.readInstance(ctx, c.instanceID).Connection
		record.observe("%s: connection %s streams from %s; lease_expires_at %s", c.m.name, leases[c.instanceID].ID, c.stream.backend, leases[c.instanceID].LeaseExpiresAt.Format(time.RFC3339Nano))
	}
	killedAt := time.Now()
	e.killHub(killed)
	record.observe("killed %s with SIGKILL at %s, after taking it out of the edge's rotation", killed, killedAt.Format(time.RFC3339Nano))

	for _, c := range clis {
		lease := leases[c.instanceID]
		if c.stream.backend != killed {
			continue
		}
		resumed := e.edge.waitEvents(t, c.instanceID, c.streams+1, 45*time.Second, "an event request after the kill", func(eventsRequest) bool { return true })
		switch {
		case resumed.status != http.StatusOK || resumed.resume != "true" || resumed.previous != lease.ID:
			t.Fatalf("%s: event request after the kill %+v; want 200 with resume=true and previous_connection_id %s", c.m.name, resumed, lease.ID)
		case resumed.backend == killed:
			t.Fatalf("%s: resumed on the killed %s", c.m.name, killed)
		case !resumed.at.Before(lease.LeaseExpiresAt):
			t.Fatalf("%s: resumed at %s, after the lease expired at %s", c.m.name, resumed.at.Format(time.RFC3339Nano), lease.LeaseExpiresAt.Format(time.RFC3339Nano))
		}
		beat := requireOnlineUntil(t, controller, c.instanceID, lease.ID, 30*time.Second, "a heartbeat accepted after the resume", func(object instanceObject) bool {
			return object.Connection.LastHeartbeatAt != nil && object.Connection.LastHeartbeatAt.After(resumed.at)
		})
		requireCLIConnection(t, c.m, lease.ID)
		record.observe("%s: resumed connection %s on %s %s after the kill, %s before its lease expired; online in every instance read from the kill on; heartbeat accepted at %s; the CLI's status online with the same connection",
			c.m.name, lease.ID, resumed.backend, resumed.at.Sub(killedAt).Round(time.Millisecond), lease.LeaseExpiresAt.Sub(resumed.at).Round(time.Millisecond), beat.Connection.LastHeartbeatAt.Format(time.RFC3339Nano))
	}
	for _, c := range clis {
		if c.stream.backend == killed {
			continue
		}
		lease := leases[c.instanceID]
		requireOnlineUntil(t, controller, c.instanceID, lease.ID, 30*time.Second, "a heartbeat accepted after the kill", func(object instanceObject) bool {
			return object.Connection.LastHeartbeatAt != nil && object.Connection.LastHeartbeatAt.After(killedAt)
		})
		if requests := e.edge.eventsRequests(c.instanceID); len(requests) != c.streams {
			t.Fatalf("%s: event requests after the kill %+v; its stream on %s should stay", c.m.name, requests[c.streams:], c.stream.backend)
		}
		record.observe("%s: its stream on %s stayed; no event request after the kill; heartbeat accepted after it", c.m.name, c.stream.backend)
	}
}

// TestStreamOnlyDisconnectResumesTheSameConnection sets computer-hub's
// stream age to 20 seconds, so the CLI's stream ends with a stream-only
// disconnect 20 to 22 seconds after its connected. The CLI resumes the same
// connection, which stays online throughout and accepts a heartbeat after
// the resume.
func TestStreamOnlyDisconnectResumesTheSameConnection(t *testing.T) {
	in := requireInputs(t, needs{})
	record := newAcceptanceRecord(t, in, platformOf(in))
	record.boundary("no command runs across the disconnect: no Engine path routes a tool call to a computer")
	record.boundary("the stream age is 20 seconds instead of the default 30 minutes")
	e := startEngine(t, record, engineOptions{hubPods: 2, streamMaxAge: "20s"})
	controller := e.controller()
	set := newMachines(t, in, needs{}, e, record)
	ctx := t.Context()

	registered := controller.createUserInstance(ctx, "aged")
	m := set.add("aged", "aged-app")
	m.configure(bootstrap(e.serverURL, registered.instance.ID, registered.credential))
	m.serve()
	requireOnline(t, controller, m, registered.instance.ID, registered.credential.ID)
	connection := *controller.readInstance(ctx, registered.instance.ID).Connection
	streams := len(e.edge.eventsRequests(registered.instance.ID))
	first := e.edge.waitEvents(t, registered.instance.ID, streams, 40*time.Second, "the end of the first stream", func(request eventsRequest) bool {
		return !request.ended.IsZero()
	})
	if age := first.ended.Sub(first.at); age < 20*time.Second || age > 23*time.Second {
		t.Fatalf("the stream from %s ended %s after its request; want 20 to 22 seconds after its connected", first.backend, age)
	}
	resumed := e.edge.waitEvents(t, registered.instance.ID, streams+1, 45*time.Second, "an event request after the stream ended", func(eventsRequest) bool { return true })
	if resumed.status != http.StatusOK || resumed.resume != "true" || resumed.previous != connection.ID {
		t.Fatalf("event request after the stream ended %+v; want 200 with resume=true and previous_connection_id %s", resumed, connection.ID)
	}
	beat := requireOnlineUntil(t, controller, registered.instance.ID, connection.ID, 30*time.Second, "a heartbeat accepted after the resume", func(object instanceObject) bool {
		return object.Connection.LastHeartbeatAt != nil && object.Connection.LastHeartbeatAt.After(resumed.at)
	})
	requireCLIConnection(t, m, connection.ID)
	record.observe("the stream from %s ended %s after its request; the CLI resumed connection %s on %s %s after that end; online in every instance read; heartbeat accepted at %s; the CLI's status online with the same connection",
		first.backend, first.ended.Sub(first.at).Round(time.Millisecond), connection.ID, resumed.backend, resumed.at.Sub(first.ended).Round(time.Millisecond), beat.Connection.LastHeartbeatAt.Format(time.RFC3339Nano))
}

// TestDelayedReadyAcknowledgementKeepsTheConnection gives 70 Sessions each a
// user computer on one offline instance, then holds every answer to
// POST /v1/client/ready at the edge for 8 seconds, inside the CLI's 10-second
// request deadline. The CLI reaches online on its first connection once the
// acknowledgement arrives and opens no other connection. The ready prepares
// all 70 computers: Engine sends at most 64 of their session.prepare
// instructions before it stores the connection's first Session report and
// the rest after it, and every computer becomes ready.
func TestDelayedReadyAcknowledgementKeepsTheConnection(t *testing.T) {
	in := requireInputs(t, needs{})
	record := newAcceptanceRecord(t, in, platformOf(in))
	e := startEngine(t, record, engineOptions{})
	e.edge.delay("/v1/client/ready", 8*time.Second)
	controller, developer := e.controller(), e.developer()
	set := newMachines(t, in, needs{}, e, record)
	ctx := t.Context()

	registered := controller.createUserInstance(ctx, "delayed")
	m := set.add("delayed", "delayed-app")
	m.configure(bootstrap(e.serverURL, registered.instance.ID, registered.credential))
	work := m.directory("work")
	if code, _ := m.shell("mkdir", "-p", work); code != 0 {
		t.Fatalf("mkdir %s on %s exited %d", work, m.name, code)
	}
	const sessions = 70
	agentID, cloud := developer.createAgent(ctx, "delayed"), developer.unservedCloudComputer(ctx, "delayed")
	for range sessions {
		developer.createSession(ctx, agentID, cloud, map[string]any{"instance_id": registered.instance.ID, "working_directory": work})
	}
	record.observe("%d Sessions created, each with an unserved cloud computer and a user computer on the offline instance %s in %s", sessions, registered.instance.ID, work)
	m.serve()
	if object := waitOnline(t, controller, m, registered.instance.ID, registered.credential.ID); object.ComputerCount != sessions {
		t.Fatalf("instance read computer_count %d; want %d", object.ComputerCount, sessions)
	}
	online := time.Now()
	requests := e.edge.eventsRequests(registered.instance.ID)
	if len(requests) != 1 || requests[0].status != http.StatusOK || requests[0].previous != "" {
		t.Fatalf("event requests %+v; want one new connection", requests)
	}
	if waited := online.Sub(requests[0].at); waited < 8*time.Second {
		t.Fatalf("online %s after the event request; the acknowledgement was held 8 seconds", waited)
	}
	object := controller.readInstance(ctx, registered.instance.ID)
	record.observe("one event request; the CLI's status was online %s after it, ready_at %s; connection %s", online.Sub(requests[0].at).Round(time.Millisecond), object.Connection.ReadyAt.Format(time.RFC3339Nano), object.Connection.ID)

	deadline := time.Now().Add(2 * time.Minute)
	var ready, before, after int
	for {
		e.ownerTx(func(ctx context.Context, tx *dbconnect.Tx) error {
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM computers WHERE current_instance_id=$1 AND execution_state='ready'`, registered.instance.ID).Scan(&ready); err != nil {
				return err
			}
			return tx.QueryRow(ctx, `SELECT count(*) FILTER (WHERE i.first_written_at < c.first_session_report_at), count(*) FILTER (WHERE i.first_written_at >= c.first_session_report_at)
				FROM connection_instructions i JOIN instance_connections c ON c.workspace_id=i.workspace_id AND c.id=i.connection_id
				WHERE c.id=$1 AND i.type='session.prepare'`, object.Connection.ID).Scan(&before, &after)
		})
		if ready == sessions || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Second)
	}
	if ready != sessions || before > 64 || before+after != sessions {
		t.Fatalf("%d of %d computers ready; session.prepare sent %d before the first Session report and %d after; want all ready, at most 64 before and the rest after", ready, sessions, before, after)
	}
	record.observe("all %d computers ready; of their session.prepare instructions on %s, %d were first written before the connection's first Session report was stored and %d after it", sessions, object.Connection.ID, before, after)
}

// TestLiveRenewalMovesTheConnectionToTheNewCredential has the Controller
// issue a credential with delivery stream to an online CLI. The CLI fetches
// it and resumes the same connection with it, which stays online throughout:
// the new credential becomes the connection's, active and in use, and the old
// one is revoked. A heartbeat
// with the old credential's key, as from a copied credential file, then
// answers 401 credential_revoked, and the instance read shows that
// credential's reuse while the connection stays online on the new one.
func TestLiveRenewalMovesTheConnectionToTheNewCredential(t *testing.T) {
	in := requireInputs(t, needs{})
	record := newAcceptanceRecord(t, in, platformOf(in))
	record.boundary("no binding and no task are on the connection during the renewal: the test creates no Session, and no Engine path routes a tool call to a computer")
	e := startEngine(t, record, engineOptions{})
	controller := e.controller()
	set := newMachines(t, in, needs{}, e, record)
	ctx := t.Context()

	registered := controller.createUserInstance(ctx, "renewed")
	old := registered.credential
	m := set.add("renewed", "renewed-app")
	m.configure(bootstrap(e.serverURL, registered.instance.ID, old))
	m.serve()
	requireOnline(t, controller, m, registered.instance.ID, old.ID)
	connection := *controller.readInstance(ctx, registered.instance.ID).Connection
	streams := len(e.edge.eventsRequests(registered.instance.ID))

	renewed := controller.issueStreamCredential(ctx, registered.instance.ID)
	issued := time.Now()
	resumed := e.edge.waitEvents(t, registered.instance.ID, streams+1, 30*time.Second, "an event request after the issuance", func(eventsRequest) bool { return true })
	if resumed.status != http.StatusOK || resumed.resume != "true" || resumed.previous != connection.ID || resumed.credential != renewed.ID {
		t.Fatalf("event request after the issuance %+v; want 200 with resume=true, previous_connection_id %s and credential_id %s", resumed, connection.ID, renewed.ID)
	}
	object := requireOnlineUntil(t, controller, registered.instance.ID, connection.ID, 30*time.Second, "a heartbeat accepted on the new credential", func(object instanceObject) bool {
		return object.Connection.CredentialID == renewed.ID && object.Connection.LastHeartbeatAt != nil && object.Connection.LastHeartbeatAt.After(resumed.at)
	})
	states := map[string]credentialObject{}
	for _, credential := range controller.listCredentials(ctx, registered.instance.ID) {
		states[credential.ID] = credential
	}
	if got := states[renewed.ID]; got.State != "active" || !got.InUse {
		t.Fatalf("renewed credential %+v; want active and in use", got)
	}
	if got := states[old.ID]; got.State != "revoked" || got.InUse {
		t.Fatalf("old credential %+v; want revoked and not in use", got)
	}
	status := m.waitStatus(10*time.Second, "credential_id "+renewed.ID, func(status map[string]any) bool {
		return statusField(status, "credential_id") == renewed.ID
	})
	if statusField(status, "connection_state") != "online" || statusField(status, "connection_id") != connection.ID {
		t.Fatalf("%s status %v; want online with connection %s", m.name, status, connection.ID)
	}
	record.observe("the CLI resumed connection %s on %s with credential %s %s after the issuance; online in every instance read; heartbeat accepted on it at %s; credential list: %s active and in use, %s revoked; the CLI's status online with the same connection and the new credential",
		connection.ID, resumed.backend, renewed.ID, resumed.at.Sub(issued).Round(time.Millisecond), object.Connection.LastHeartbeatAt.Format(time.RFC3339Nano), renewed.ID, old.ID)

	copied := enginetest.CLI{BaseURL: e.serverURL, InstanceID: registered.instance.ID, CredentialID: old.ID, Key: *old.ConnectionKey}
	used := time.Now()
	response, err := copied.Heartbeat(ctx, protocol.Heartbeat{ConnectionID: connection.ID, MessageID: "copied-heartbeat", RequestMessageID: "copied-heartbeat-request", HeartbeatSeq: 1})
	if err != nil {
		t.Fatal(err)
	}
	refusal, err := response.Error()
	if err != nil || response.Status != http.StatusUnauthorized || refusal.Code != protocol.ErrorCredentialRevoked {
		t.Fatalf("heartbeat with the old credential answered %d %s (%v); want 401 credential_revoked", response.Status, response.Body, err)
	}
	object = controller.readInstance(ctx, registered.instance.ID)
	switch {
	case object.LastCredentialReuse == nil || object.LastCredentialReuse.CredentialID != old.ID:
		t.Fatalf("instance read last_credential_reuse %+v; want credential %s", object.LastCredentialReuse, old.ID)
	case object.Connection == nil || object.Connection.ID != connection.ID || object.Connection.State != "online" || object.Connection.CredentialID != renewed.ID:
		t.Fatalf("instance read connection %+v after the old credential's use; want %s online on %s", object.Connection, connection.ID, renewed.ID)
	}
	record.observe("a heartbeat with the old credential's key through the edge %s after the renewal answered 401 credential_revoked; the instance read's last_credential_reuse names %s, detected_at %s; the connection stayed online on %s",
		used.Sub(resumed.at).Round(time.Millisecond), old.ID, object.LastCredentialReuse.DetectedAt.Format(time.RFC3339Nano), renewed.ID)
}

// requireOnlineUntil reads instanceID until done accepts the read, failing
// as soon as a read does not show connectionID online.
func requireOnlineUntil(t *testing.T, controller *controllerClient, instanceID, connectionID string, timeout time.Duration, what string, done func(instanceObject) bool) instanceObject {
	t.Helper()
	return controller.waitInstance(t.Context(), instanceID, timeout, what, func(object instanceObject) bool {
		if object.Connection == nil || object.Connection.ID != connectionID || object.Connection.State != "online" {
			t.Fatalf("instance %s read connection %+v; want %s online", instanceID, object.Connection, connectionID)
		}
		return done(object)
	})
}

// requireCLIConnection checks that the machine's status is online with
// connectionID.
func requireCLIConnection(t *testing.T, m *machine, connectionID string) {
	t.Helper()
	status := m.waitStatus(10*time.Second, "connection_state online", func(status map[string]any) bool {
		return statusField(status, "connection_state") == "online"
	})
	if statusField(status, "connection_id") != connectionID {
		t.Fatalf("%s status names connection %s; want %s", m.name, status["connection_id"], connectionID)
	}
}

// TestEachPreparationFetchesANewGitTicket prepares a cloud computer with a
// repository, stops its machine and wakes it, which prepares the computer
// again. Each preparation fetches one git ticket and no session.prepare
// carries one; the second preparation reuses the first clone with the new
// ticket in its configuration, and a git fetch in it reaches the repository
// through git-proxy. git-proxy refuses the first ticket and accepts the
// second, and Engine holds only the tickets' hashes, the first revoked.
func TestEachPreparationFetchesANewGitTicket(t *testing.T) {
	in := requireInputs(t, needs{})
	record := newAcceptanceRecord(t, in, platformOf(in))
	record.boundary("the requests are recorded in-process with computer.EnsureRequest; no machine-bound call waits for them, since no Engine path routes a tool call to a computer")
	record.boundary("the machine is stopped with SIGTERM, not by its idle exit")
	e := startEngine(t, record, engineOptions{})
	controller, developer := e.controller(), e.developer()
	set := newMachines(t, in, needs{}, e, record)
	ctx := t.Context()

	const token = "ghs_harness_repository_token"
	commit := e.github.createRepository("acme", "app", token)
	cloud := set.add("cloud", "provider-launcher")
	environmentID := developer.createEnvironment(ctx, "tickets")
	session := developer.createSession(ctx, developer.createAgent(ctx, "tickets"), map[string]any{
		"environment_id": environmentID, "working_directory": cloud.directory("workspace"),
		"repositories": []any{map[string]any{"url": "https://github.com/acme/app", "authorization_token": token}},
	})
	s := cloudSession{sessionID: session.ID, computerID: session.Computers[0].ID, environmentID: environmentID}
	clone := session.Computers[0].Repositories[0].Path
	server, instanceID := startClaimedMachine(t, e, cloud, s)
	if code, head := cloud.shell("git", "-C", clone, "rev-parse", "HEAD"); code != 0 || head != commit {
		t.Fatalf("the clone at %s: exit %d, HEAD %s; want the repository's commit %s", clone, code, head, commit)
	}
	first := cloneTicket(t, cloud, clone, e.gitProxyURL)
	record.observe("the first preparation cloned https://github.com/acme/app to %s at %s through git-proxy", clone, commit)

	stopMachine(t, controller, server, instanceID)
	wake := e.recordRequest(s)
	claimed := controller.claimRequest(ctx, wake.ID, "worker-wake")
	_, wakeCredential := claimed.bootstrapFields(t)
	cloud.configure(claimed.Bootstrap)
	cloud.serve("--idle-exit", "15m")
	requireCloudOnline(t, e, cloud, instanceID, wakeCredential, s)
	second := cloneTicket(t, cloud, clone, e.gitProxyURL)
	if second == first {
		t.Fatal("the second preparation left the first ticket in the clone's configuration")
	}
	fetched := len(e.github.requestsOf("acme/app"))
	if code, _ := cloud.shell("git", "-C", clone, "fetch", "origin"); code != 0 {
		t.Fatalf("git fetch in the reused clone exited %d", code)
	}
	if origin, requests := gitOutput(t, cloud, "-C", clone, "config", "--local", "--get", "remote.origin.url"), e.github.requestsOf("acme/app"); origin != "https://github.com/acme/app" || len(requests) == fetched {
		t.Fatalf("origin %s, GitHub requests %d before and %d after the fetch; want https://github.com/acme/app and the fetch reaching GitHub", origin, fetched, len(requests))
	}
	for _, request := range e.github.requestsOf("acme/app") {
		if !request.authorized {
			t.Fatalf("GitHub request %+v without the repository's token", request)
		}
	}
	record.observe("the wake's preparation reused the clone and replaced its ticket; git fetch in it reached GitHub through git-proxy with origin https://github.com/acme/app; every GitHub request carried the repository's token")

	for ticket, want := range map[string]int{first: http.StatusUnauthorized, second: http.StatusOK} {
		if status := gitProxyInfoRefs(t, e, ticket); status != want {
			t.Fatalf("git-proxy answered %d to the %s ticket; want %d", status, map[bool]string{true: "first", false: "second"}[ticket == first], want)
		}
	}
	firstHash, _ := gitticket.HashToken(first)
	secondHash, _ := gitticket.HashToken(second)
	var tickets, revokedFirst, liveSecond, plaintext, prepares, carrying int
	e.ownerTx(func(ctx context.Context, tx *dbconnect.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE token_hash=$2 AND state='revoked'), count(*) FILTER (WHERE token_hash=$3 AND state='live'),
			count(*) FILTER (WHERE strpos(row_to_json(t)::text,$4)>0 OR strpos(row_to_json(t)::text,$5)>0)
			FROM computer_git_tickets t WHERE computer_id=$1`, s.computerID, firstHash, secondHash, first, second).Scan(&tickets, &revokedFirst, &liveSecond, &plaintext); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE strpos(convert_from(i.envelope,'UTF8'),'ticket')>0)
			FROM connection_instructions i JOIN instance_connections c ON c.workspace_id=i.workspace_id AND c.id=i.connection_id
			WHERE c.instance_id=$1 AND i.type='session.prepare'`, instanceID).Scan(&prepares, &carrying)
	})
	if tickets != 2 || revokedFirst != 1 || liveSecond != 1 || plaintext != 0 || prepares != 2 || carrying != 0 {
		t.Fatalf("tickets %d (first revoked %d, second live %d, plaintext %d); session.prepare %d, %d naming a ticket; want 2 (1, 1, 0) and 2, 0", tickets, revokedFirst, liveSecond, plaintext, prepares, carrying)
	}
	record.observe("git-proxy answered 401 to the first ticket and 200 to the second; computer_git_tickets holds 2 rows of the computer, the first ticket's hash revoked and the second's live, neither ticket in plaintext; the instance's 2 session.prepare instructions name no ticket")
}

// cloneTicket is the git ticket of the clone's local configuration, the
// header value git sends to gitProxyURL.
func cloneTicket(t *testing.T, m *machine, clone, gitProxyURL string) string {
	t.Helper()
	header := gitOutput(t, m, "-C", clone, "config", "--local", "--get", "http."+gitProxyURL+"/.extraheader")
	ticket, found := strings.CutPrefix(header, "X-Tetral-Git-Ticket: ")
	if !found || ticket == "" {
		t.Fatalf("the clone %s holds no ticket header for %s", clone, gitProxyURL)
	}
	return ticket
}

// gitOutput runs git with args on the machine and returns its stdout,
// failing t when it fails.
func gitOutput(t *testing.T, m *machine, args ...string) string {
	t.Helper()
	code, out := m.shell(append([]string{"git"}, args...)...)
	if code != 0 {
		t.Fatalf("git %s on %s exited %d", strings.Join(args, " "), m.name, code)
	}
	return out
}

// gitProxyInfoRefs sends git-proxy, from the test process, the first request
// of a clone of acme/app with ticket and returns its status.
func gitProxyInfoRefs(t *testing.T, e *engine, ticket string) int {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(e.github.authority.PEM)
	client := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, e.gitProxyURL+"/github.com/acme/app.git/info/refs?service=git-upload-pack", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Tetral-Git-Ticket", ticket)
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("git-proxy info/refs: %v", err)
	}
	_ = response.Body.Close()
	e.record.command("`GET %s/github.com/acme/app.git/info/refs?service=git-upload-pack` with a ticket answered %d", e.gitProxyURL, response.StatusCode)
	return response.StatusCode
}
