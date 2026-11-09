package joint

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/enginetest"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/gitticket"
	"github.com/tetral-ai/tetral/internal/protocol"
)

// TestClientConnection runs the client connection's scenarios of
// credentials, streams and readiness on one Engine with two computer-hub
// pods.
func TestClientConnection(t *testing.T) {
	scenarioTable(t, engineOptions{hubPods: 2}, needs{},
		tableRow("live_renewal", liveRenewalMovesTheConnectionToTheNewCredential).budget(25, 30),
		tableRow("delayed_ready_acknowledgement", delayedReadyAcknowledgementKeepsTheConnection).budget(35, 40),
		tableRow("cut_connection", cutConnectionEndsItsCalls).budget(70, 70),
		tableRow("git_ticket_per_preparation", eachPreparationFetchesANewGitTicket).budget(70, 75),
		tableRow("required_repository_failure", requiredRepositoryFailureReachesSDK).only("linux").budget(25, 0),
		tableRow("killed_hub_pod", killedHubPodsCLIsResumeOnOtherPods).budget(40, 45),
		tableRow("restart_after_server_stop", restartAfterServerStopConnectsAtOnce).budget(60, 60),
		tableRow("restart_after_sigkill", restartAfterSIGKILLWaitsForTheLease).budget(120, 120),
	)
}

// TestLostChanges runs the scenarios in which drive changes a CLI holds are
// lost, by a close, a late close or a drive's removal, and reported, on one
// Engine with three computer-hub pods.
func TestLostChanges(t *testing.T) {
	scenarioTable(t, engineOptions{hubPods: 3}, needs{},
		tableRow("late_close", lateCloseListsEachDrivesLostChanges).budget(100, 110),
		tableRow("late_close_64_KiB", lateClosedReportFitsIn64KiB).budget(100, 110),
		tableRow("close_with_held_upload", closeListsHeldUploadsAndRejectsConflicts).budget(60, 65),
		tableRow("removed_drive", removedDrivesLostChangesComeWithTheNextReady).budget(140, 145),
		tableRow("close_then_reconnects", lostChangesOfACloseSurviveReconnects).budget(330, 335),
	)
}

// killedHubPodsCLIsResumeOnOtherPods runs two CLIs against two
// computer-hub pods, one stream on each, with a command running on a
// Session's user computer of the first CLI, and kills the pod that holds the
// first CLI's stream, with no drain. While the edge holds the first CLI's
// next event request, a second command is admitted to that computer. The
// first CLI resumes the same connection on the other pod before its lease
// expires; its connection stays online throughout and accepts a heartbeat
// after the resume; the queued command is delivered and runs, and the
// running one goes on to finish. The second CLI keeps its stream on the
// other pod.
func killedHubPodsCLIsResumeOnOtherPods(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	controller := e.controller()
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
	session, computerID := userComputerSession(t, e, clis[0].m, clis[0].instanceID, "tasks")
	rs := e.startRuntime().bind(session.ID)
	running, release := rs.heldTask(clis[0].m, computerID, clis[0].m.directory("tasks"), "running")
	leases := map[string]connectionObject{}
	for _, c := range clis {
		leases[c.instanceID] = *controller.readInstance(ctx, c.instanceID).Connection
		record.observe("%s: connection %s streams from %s; lease_expires_at %s", c.m.name, leases[c.instanceID].ID, c.stream.backend, leases[c.instanceID].LeaseExpiresAt.Format(time.RFC3339Nano))
	}
	hold := e.edge.holdEvents(clis[0].instanceID)
	killedAt := time.Now()
	e.killHub(killed)
	record.observe("killed %s with SIGKILL at %s, after taking it out of the edge's rotation", killed, killedAt.Format(time.RFC3339Nano))
	hold.waitArrived(t, 30*time.Second)
	queued := rs.exec(computerID, "echo queued")
	e.edge.releaseEvents(clis[0].instanceID)
	record.observe("%s's first event request after the kill was held at the edge while command %s was admitted, then released", clis[0].m.name, queued.use)

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
	rs.requireCompleted(queued, time.Minute, "queued")
	if code, _ := clis[0].m.shell("touch", release); code != 0 {
		t.Fatalf("touch %s exited %d", release, code)
	}
	rs.requireCompleted(running, time.Minute, "finished")
	record.observe("on %s, the command admitted while no stream was in place ran after the resume, and the command running across the kill finished with its terminal result", clis[0].m.name)
}

// TestStreamOnlyDisconnectResumesTheSameConnection sets computer-hub's
// stream age to 20 seconds, so the CLI's stream ends with a stream-only
// disconnect 20 to 22 seconds after its connected. The CLI resumes the same
// connection, which stays online throughout and accepts a heartbeat after
// the resume.
func TestStreamOnlyDisconnectResumesTheSameConnection(t *testing.T) {
	ownBudget(t, 55, 60)
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

// delayedReadyAcknowledgementKeepsTheConnection gives 70 Sessions each a
// user computer on one offline instance, then holds every answer to
// POST /v1/client/ready at the edge for 8 seconds, inside the CLI's 10-second
// request deadline. The CLI reaches online on its first connection once the
// acknowledgement arrives and opens no other connection. The ready prepares
// all 70 computers: Engine sends at most 64 of their session.prepare
// instructions before it stores the connection's first Session report and
// the rest after it, and every computer becomes ready.
func delayedReadyAcknowledgementKeepsTheConnection(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	e.edge.delay("/v1/client/ready", 8*time.Second)
	controller, developer := e.controller(), e.developer()
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

// liveRenewalMovesTheConnectionToTheNewCredential has the Controller
// issue a credential with delivery stream to an online CLI while a command
// runs on a Session's user computer there. The CLI fetches it and resumes
// the same connection with it, which stays online throughout: the new
// credential becomes the connection's, active and in use, and the old one is
// revoked; the computer keeps its binding and the command goes on to finish
// with its result. A heartbeat with the old credential's key, as from a
// copied credential file, then answers 401 credential_revoked, and the
// instance read shows that credential's reuse while the connection stays
// online on the new one.
func liveRenewalMovesTheConnectionToTheNewCredential(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	controller := e.controller()
	ctx := t.Context()

	registered := controller.createUserInstance(ctx, "renewed")
	old := registered.credential
	m := set.add("renewed", "renewed-app")
	m.configure(bootstrap(e.serverURL, registered.instance.ID, old))
	m.serve()
	requireOnline(t, controller, m, registered.instance.ID, old.ID)
	connection := *controller.readInstance(ctx, registered.instance.ID).Connection
	session, computerID := userComputerSession(t, e, m, registered.instance.ID, "renewal")
	bound := e.developer().readSessionComputer(ctx, session.ID, computerID)
	rs := e.startRuntime().bind(session.ID)
	task, release := rs.heldTask(m, computerID, m.directory("renewal"), "renewal")
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
	if after := e.developer().readSessionComputer(ctx, session.ID, computerID); after.ExecutionState != "ready" || after.BindingID == nil || *after.BindingID != *bound.BindingID {
		t.Fatalf("computer %s after the renewal: %+v; want ready on binding %s", computerID, after, *bound.BindingID)
	}
	if code, _ := m.shell("touch", release); code != 0 {
		t.Fatalf("touch %s exited %d", release, code)
	}
	rs.requireCompleted(task, time.Minute, "finished")
	record.observe("computer %s still ready on binding %s after the renewal; the command that ran across it finished with its terminal result", computerID, *bound.BindingID)

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

// eachPreparationFetchesANewGitTicket prepares a cloud computer with
// nested repositories submitted inner-first, then stops and wakes it, preparing
// the computer
// again. Each preparation fetches one git ticket and no session.prepare
// carries one; the second preparation reuses the first clone with the new
// ticket in its configuration, and a git fetch in it reaches the repository
// through git-proxy. git-proxy refuses the first ticket and accepts the
// second, and Engine holds only the tickets' hashes, the first revoked.
func eachPreparationFetchesANewGitTicket(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	record.boundary("the machine is stopped with SIGTERM, not by its idle exit")
	controller, developer := e.controller(), e.developer()
	ctx := t.Context()

	const token = "ghs_harness_repository_token"
	commit := e.github.createRepository("acme", "app", token)
	innerCommit := e.github.createRepository("acme", "lib", token)
	cloud := set.add("cloud", "provider-launcher")
	environmentID := developer.createEnvironment(ctx, "tickets")
	session := developer.createSession(ctx, developer.createAgent(ctx, "tickets"), map[string]any{
		"environment_id": environmentID, "working_directory": cloud.directory("workspace"),
		"repositories": []any{
			map[string]any{"url": "https://github.com/acme/lib", "authorization_token": token, "mount_path": cloud.directory("workspace") + "/app/lib"},
			map[string]any{"url": "https://github.com/acme/app", "authorization_token": token},
		},
	})
	s := cloudSession{sessionID: session.ID, computerID: session.Computers[0].ID, environmentID: environmentID}
	if repositories := session.Computers[0].Repositories; len(repositories) != 2 || repositories[0].Path != cloud.directory("workspace")+"/app/lib" || repositories[1].Path != cloud.directory("workspace")+"/app" {
		t.Fatalf("API repository order changed: %+v", repositories)
	}
	innerClone := session.Computers[0].Repositories[0].Path
	clone := session.Computers[0].Repositories[1].Path
	rs := e.startRuntime().bind(s.sessionID)
	server, instanceID := startClaimedMachine(t, e, rs, cloud, s)
	if code, head := cloud.shell("git", "-C", clone, "rev-parse", "HEAD"); code != 0 || head != commit {
		t.Fatalf("the clone at %s: exit %d, HEAD %s; want the repository's commit %s", clone, code, head, commit)
	}
	if code, head := cloud.shell("git", "-C", innerClone, "rev-parse", "HEAD"); code != 0 || head != innerCommit {
		t.Fatalf("nested clone: exit %d, HEAD %s; want %s", code, head, innerCommit)
	}
	first := cloneTicket(t, cloud, clone, e.gitProxyURL)
	if innerTicket := cloneTicket(t, cloud, innerClone, e.gitProxyURL); innerTicket != first {
		t.Fatal("nested repositories did not use the same preparation ticket")
	}
	record.observe("the first preparation cloned https://github.com/acme/app to %s at %s through git-proxy", clone, commit)
	record.observe("inner-first API order stayed intact, while preparation cloned the outer repository before its nested acme/lib checkout at %s (%s)", innerClone, innerCommit)

	stopMachine(t, controller, server, instanceID)
	call, wake := waitingCall(t, e, rs, s, "pwd")
	claimed := controller.claimRequest(ctx, wake.ID, "worker-wake")
	_, wakeCredential := claimed.bootstrapFields(t)
	cloud.configure(claimed.Bootstrap)
	cloud.serve("--idle-exit", "15m")
	requireCloudOnline(t, e, cloud, instanceID, wakeCredential, s)
	rs.requireCompleted(call, time.Minute, cloud.directory("workspace"))
	second := cloneTicket(t, cloud, clone, e.gitProxyURL)
	if second == first {
		t.Fatal("the second preparation left the first ticket in the clone's configuration")
	}
	if innerTicket := cloneTicket(t, cloud, innerClone, e.gitProxyURL); innerTicket != second {
		t.Fatal("repreparation did not replace the nested clone's ticket")
	}
	if code, head := cloud.shell("git", "-C", innerClone, "rev-parse", "HEAD"); code != 0 || head != innerCommit {
		t.Fatalf("nested checkout changed on repreparation: exit %d, HEAD %s", code, head)
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
		return tx.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE strpos(convert_from(i.envelope,'UTF8'),$2)>0 OR strpos(convert_from(i.envelope,'UTF8'),$3)>0)
			FROM connection_instructions i JOIN instance_connections c ON c.workspace_id=i.workspace_id AND c.id=i.connection_id
			WHERE c.instance_id=$1 AND i.type='session.prepare'`, instanceID, first, second).Scan(&prepares, &carrying)
	})
	if tickets != 2 || revokedFirst != 1 || liveSecond != 1 || plaintext != 0 || prepares != 2 || carrying != 0 {
		t.Fatalf("tickets %d (first revoked %d, second live %d, plaintext %d); session.prepare %d, %d carrying a ticket; want 2 (1, 1, 0) and 2, 0", tickets, revokedFirst, liveSecond, plaintext, prepares, carrying)
	}
	record.observe("git-proxy answered 401 to the first ticket and 200 to the second; computer_git_tickets holds 2 rows of the computer, the first ticket's hash revoked and the second's live, neither ticket in plaintext; the instance's 2 session.prepare instructions carry neither ticket")
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

// lateClosed runs scripts, the i-th in the i-th of as many read_write drives
// of a Session's user computer, with every upload refused; then it stops the
// App Server, removes the computer while its instance is offline and starts
// the App Server again, so that the CLI gets the close late. Once the
// computer closed, with no upload after the restart and the Session's data
// deleted, it returns the drives, the Session and the CLI's accepted closed
// report.
func lateClosed(t *testing.T, e *engine, set *machines, scripts ...string) ([]string, string, capturedReport, protocol.SessionStatus) {
	t.Helper()
	ctx := t.Context()
	developer := e.developer()
	m := onlineUserMachine(t, e, set, "late")
	var drives []string
	var bound []sessionDrive
	for range scripts {
		driveID := developer.createDrive(ctx)
		drives = append(drives, driveID)
		bound = append(bound, readWrite(driveID))
	}
	session, computers, rs := driveSession(t, e, "late", bound, m)
	commands := make([]string, len(scripts))
	for i, script := range scripts {
		refuseUploads(e, drives[i])
		commands[i] = `(cd "$TETRAL_DRIVES/` + drives[i] + `" && ` + script + `)`
	}
	// The writes can outlast the default 10-second wait on a loaded disk;
	// the command still has to end with its terminal report.
	written := rs.terminal(rs.execInput(computers[0], map[string]any{"cmd": strings.Join(commands, " && "), "wait_ms": 50000}), 5*time.Minute)
	if written.Result.ExitCode == nil || *written.Result.ExitCode != 0 {
		t.Fatalf("the writing command ended %+v; want exit 0", written)
	}
	m.server.stop()
	m.waitOffline(e)
	if status, removed := developer.removeComputer(ctx, session.ID, computers[0]); status != http.StatusAccepted || removed.ExecutionState != "unknown" {
		t.Fatalf("removing the computer answered %d %+v; want 202 unknown", status, removed)
	}
	restarted := time.Now()
	m.server = m.serve()
	captured, report := e.waitReport(2*time.Minute, "closed report", func(report protocol.SessionStatus) bool {
		return report.Status == "closed" && report.SessionID == session.ID
	})
	var kind, closeID string
	e.ownerTx(func(ctx context.Context, tx *dbconnect.Tx) error {
		return tx.QueryRow(ctx, `SELECT b.kind,COALESCE(b.close_message_id,'') FROM computers c JOIN computer_bindings b ON b.workspace_id=c.workspace_id AND b.binding_id=c.binding_id WHERE c.workspace_id=$1 AND c.id=$2`, harnessWorkspace, computers[0]).Scan(&kind, &closeID)
	})
	if kind != "late_close" || closeID != report.RequestMessageID {
		t.Fatalf("computer %s's binding is %q with close %q; want the late close %s the report answers", computers[0], kind, closeID, report.RequestMessageID)
	}
	developer.waitClosed(ctx, session.ID, computers[0], 2*time.Minute)
	for _, driveID := range drives {
		for _, put := range e.edge.answered(http.MethodPut, contentRoute(driveID)) {
			if put.at.After(restarted) {
				t.Fatalf("the CLI uploaded %s after it restarted", put.path)
			}
		}
	}
	if m.sessionData(session.ID) {
		t.Fatalf("%s still holds the Session's data", m.name)
	}
	e.record.observe("the restarted CLI got late close %s, made no upload, deleted the Session's data and reported closed; the computer closed", closeID)
	return drives, session.ID, captured, report
}

// lateCloseListsEachDrivesLostChanges gives a late close a Session whose
// sync state holds two uncommitted changes in one drive and 101 in another.
// The closed report lists both paths of the first drive and the first 100
// of the second in UTF-8 byte order, with paths_total 2 and 101, and one
// drive_changes_lost_error per drive carries the same lists.
func lateCloseListsEachDrivesLostChanges(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	drives, sessionID, _, report := lateClosed(t, e, set,
		"printf x > a.txt && printf x > Z.txt",
		"printf x > ж.txt && for i in $(seq -w 0 99); do printf x > n$i; done")
	many := make([]string, 100)
	for i := range many {
		many[i] = fmt.Sprintf("n%02d", i)
	}
	want := map[string]protocol.LostChange{
		drives[0]: {DriveID: drives[0], Paths: []string{"Z.txt", "a.txt"}, PathsTotal: 2},
		drives[1]: {DriveID: drives[1], Paths: many, PathsTotal: 101},
	}
	lost := report.LostChanges.Value
	if len(lost) != 2 || lost[0].DriveID == lost[1].DriveID {
		t.Fatalf("the closed report lists lost_changes %+v; want one entry per drive", lost)
	}
	for _, entry := range lost {
		wanted := want[entry.DriveID]
		if entry.RequestMessageID != report.RequestMessageID || !slices.Equal(entry.Paths, wanted.Paths) || entry.PathsTotal != wanted.PathsTotal {
			t.Fatalf("lost_changes entry %+v; want paths %q, paths_total %d under the close %s", entry, wanted.Paths, wanted.PathsTotal, report.RequestMessageID)
		}
	}
	errs := e.developer().lostChangeErrors(t.Context(), sessionID)
	if len(errs) != 2 || errs[0].Error.DriveID == errs[1].Error.DriveID {
		t.Fatalf("drive_changes_lost_error events %+v; want one per drive", errs)
	}
	for _, err := range errs {
		wanted := want[err.Error.DriveID]
		if !slices.Equal(err.Error.Paths, wanted.Paths) || err.Error.PathsTotal != wanted.PathsTotal {
			t.Fatalf("drive_changes_lost_error %+v; want paths %q, paths_total %d", err.Error, wanted.Paths, wanted.PathsTotal)
		}
	}
	record.observe("lost_changes listed Z.txt and a.txt (paths_total 2) and n00 to n99 without ж.txt (paths_total 101); one drive_changes_lost_error per drive with the same lists")
}

// lateClosedReportFitsIn64KiB gives a late close a Session whose sync
// state holds 100 uncommitted changes with 1,000-byte paths in each of two
// drives. The accepted closed report is at most 64 KiB; both entries have
// paths_total 100; the first lists its drive's first paths in UTF-8 byte
// order, as many as fit, the second none; each of the two
// drive_changes_lost_error carries its entry's paths and paths_total.
func lateClosedReportFitsIn64KiB(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	segment := strings.Repeat("x", 240)
	directory := strings.Repeat(segment+"/", 4)
	name := strings.Repeat("y", 1000-len(directory)-2)
	// The relative cd keeps every path a system call gets under macOS's
	// 1,024-byte limit.
	script := `mkdir -p "` + directory + `" && cd "` + directory + `" && for i in $(seq -w 0 99); do printf x > "${i}` + name + `"; done`
	_, sessionID, captured, report := lateClosed(t, e, set, script, script)
	paths := make([]string, 100)
	for i := range paths {
		paths[i] = fmt.Sprintf("%s%02d%s", directory, i, name)
	}
	lost := report.LostChanges.Value
	if len(captured.body) > 64<<10 || len(lost) != 2 || lost[0].DriveID == lost[1].DriveID {
		t.Fatalf("the closed report holds %d bytes with lost_changes of %d entries; want at most 65,536 bytes and one entry per drive", len(captured.body), len(lost))
	}
	first, second := lost[0], lost[1]
	listed := len(first.Paths)
	// One more path would add its quoted text and a comma.
	if listed == 0 || !slices.Equal(first.Paths, paths[:listed]) || len(captured.body)+len(paths[listed])+3 <= 64<<10 || first.PathsTotal != 100 || len(second.Paths) != 0 || second.PathsTotal != 100 {
		t.Fatalf("the %d-byte closed report lists %d paths of %s (paths_total %d) and %d of %s (paths_total %d); want the first paths in byte order, as many as fit, then none, both paths_total 100",
			len(captured.body), listed, first.DriveID, first.PathsTotal, len(second.Paths), second.DriveID, second.PathsTotal)
	}
	errs := e.developer().lostChangeErrors(t.Context(), sessionID)
	if len(errs) != 2 {
		t.Fatalf("drive_changes_lost_error events %+v; want two", errs)
	}
	for _, err := range errs {
		entry := first
		if err.Error.DriveID == second.DriveID {
			entry = second
		}
		if err.Error.DriveID != entry.DriveID || !slices.Equal(err.Error.Paths, entry.Paths) || err.Error.PathsTotal != 100 {
			t.Fatalf("drive_changes_lost_error of %s lists %d paths with paths_total %d; want its entry's %d paths and 100", err.Error.DriveID, len(err.Error.Paths), err.Error.PathsTotal, len(entry.Paths))
		}
	}
	record.observe("the %d-byte closed report listed %d of 100 paths for the first drive and none for the second, both paths_total 100; each drive_changes_lost_error carried its entry's paths", len(captured.body), listed)
}

// cutConnectionEndsItsCalls cuts a machine's connection without a
// disconnect, by killing its App Server, while one Session's command runs
// and a call to another Session's computer waits for a preparation whose
// prepared report the edge withholds. The instance reads offline once the
// 45-second lease expires, and Cleanup's next pass ends the connection: the
// running call ends with the outcome unknown, "computer offline, result
// unknown", and the waiting call ends not executed with the computer
// offline.
func cutConnectionEndsItsCalls(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	m := onlineUserMachine(t, e, set, "cut")
	running, computers, rs := driveSession(t, e, "running", nil, m)
	// The command may outlive its App Server; it ends on its own.
	started := m.directory("running") + "/sent.started"
	sent := rs.execInput(computers[0], map[string]any{"cmd": "touch '" + started + "' && sleep 300", "wait_ms": 60000})
	for deadline := time.Now().Add(time.Minute); ; time.Sleep(200 * time.Millisecond) {
		if code, _ := m.shell("test", "-e", started); code == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the command of %s did not start within a minute", sent.use)
		}
	}
	withheld, _ := withholdReports(e, func(report protocol.SessionStatus) bool {
		return report.Status == "prepared" && report.SessionID != running.ID
	})
	work := m.directory("waiting")
	if code, _ := m.shell("mkdir", "-p", work); code != 0 {
		t.Fatalf("mkdir %s exited %d", work, code)
	}
	waiting := developer.createSession(ctx, developer.createAgent(ctx, "waiting"), developer.unservedCloudComputer(ctx, "waiting"),
		map[string]any{"instance_id": m.instanceID, "working_directory": work})
	ws := e.startRuntime().bind(waiting.ID)
	queued := ws.exec(waiting.Computers[1].ID, "true")
	select {
	case <-withheld:
	case <-time.After(time.Minute):
		t.Fatal("no prepared report of the second Session reached the edge within a minute")
	}
	record.observe("call %s runs on %s; call %s waits for the preparation of %s, whose prepared report the edge withholds", sent.use, computers[0], queued.use, waiting.Computers[1].ID)

	cut := time.Now()
	if err := m.server.signal(syscall.SIGKILL); err != nil {
		t.Fatalf("killing the App Server: %v", err)
	}
	e.controller().waitInstance(ctx, m.instanceID, 3*time.Minute, "connection offline", func(object instanceObject) bool {
		return object.Connection != nil && object.Connection.State == "offline"
	})
	offline := time.Since(cut)
	// Cleanup's pass, which deployments run every minute, ends the expired
	// connection.
	e.runCleanupPhases(nil)
	if ended := time.Since(cut); ended > 45*time.Second+time.Minute {
		t.Fatalf("the connection ended %s after the cut; want within 45 seconds plus a minute", ended)
	}
	if settled := rs.await(sent.use, time.Minute); settled.Settlement != "unknown_outcome" || settled.Message != "computer offline, result unknown" {
		t.Fatalf("the running call settled %+v; want the outcome unknown, computer offline", settled)
	}
	if settled := ws.await(queued.use, time.Minute); settled.Settlement != "not_executed" || settled.Code != "computer_offline" {
		t.Fatalf("the waiting call settled %+v; want not executed with the computer offline", settled)
	}
	record.observe("the instance read offline %s after the App Server was killed, and Cleanup's next pass ended the connection; the running call ended with the outcome unknown and the waiting call not executed, computer offline", offline.Round(time.Second))
}

// restartAfterServerStopConnectsAtOnce stops a machine's App Server with
// `server stop` and starts it again at once. The stop sends one disconnect,
// which Engine answers 200: the old connection reads ended with cause
// disconnected between the stop's start and the process's exit. The new App
// Server's first event request is answered 200, with no 409, and it reads
// online.
func restartAfterServerStopConnectsAtOnce(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	m := onlineUserMachine(t, e, set, "stopped")
	old := controllerConnection(t, e, m.instanceID)
	disconnects := disconnectsOf(e, old)
	before := len(e.edge.eventsRequests(m.instanceID))
	stopping := time.Now()
	if result := m.run(nil, "server", "stop"); result.exitCode != 0 {
		t.Fatalf("server stop exited %d: %s", result.exitCode, result.stderr)
	}
	select {
	case <-m.server.exited:
	case <-time.After(15 * time.Second):
		t.Fatal("server run did not exit within 15 seconds of server stop")
	}
	exited := time.Now()
	endedAt, cause := connectionEnd(t, e, old)
	if got := disconnects(); !slices.Equal(got, []int{http.StatusOK}) || cause != "disconnected" || endedAt.Before(stopping) || endedAt.After(exited) {
		t.Fatalf("disconnects answered %v; connection %s ended %s with cause %q; want one answered 200, and an end with cause disconnected between %s and %s", got, old, endedAt, cause, stopping, exited)
	}
	record.observe("server stop sent one disconnect of %s, answered 200; the connection ended with cause disconnected %s after the stop's start, before server run exited", old, endedAt.Sub(stopping).Round(time.Millisecond))
	m.server = m.serve()
	waitOnline(t, e.controller(), m.machine, m.instanceID, m.credentialID)
	requests := e.edge.eventsRequests(m.instanceID)[before:]
	if len(requests) != 1 || requests[0].status != http.StatusOK {
		t.Fatalf("the event requests after the restart %+v; want one, answered 200", requests)
	}
	record.observe("the restarted App Server's first event request was answered 200 and it read online on %s", controllerConnection(t, e, m.instanceID))
}

// restartAfterSIGKILLWaitsForTheLease kills a machine's App Server with
// SIGKILL and starts it again at once. The killed process sends no
// disconnect. The new App Server's event requests are answered 409 while the
// old connection's lease lasts, its status reads connection_conflict and
// never blocked, and the request after the last 409, once the lease has
// ended, is answered 200, after which it reads online.
func restartAfterSIGKILLWaitsForTheLease(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	m := onlineUserMachine(t, e, set, "killed")
	old := controllerConnection(t, e, m.instanceID)
	disconnects := disconnectsOf(e, old)
	before := len(e.edge.eventsRequests(m.instanceID))
	if err := m.server.signal(syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL to server run: %v", err)
	}
	<-m.server.exited
	m.server = m.serve()
	var conflicted bool
	m.waitStatus(2*time.Minute, "connection_state online", func(status map[string]any) bool {
		state := statusField(status, "connection_state")
		if state == "blocked" {
			t.Fatalf("the restarted App Server reads blocked: %v", status)
		}
		if last, _ := status["last_error"].(map[string]any); last != nil && last["code"] == "connection_conflict" {
			conflicted = true
		}
		return state == "online"
	})
	var lease time.Time
	e.ownerTx(func(ctx context.Context, tx *dbconnect.Tx) error {
		return tx.QueryRow(ctx, `SELECT lease_expires_at FROM instance_connections WHERE workspace_id=$1 AND id=$2`, harnessWorkspace, old).Scan(&lease)
	})
	requests := e.edge.eventsRequests(m.instanceID)[before:]
	first := slices.IndexFunc(requests, func(r eventsRequest) bool { return r.status != http.StatusConflict })
	switch {
	case len(disconnects()) != 0:
		t.Fatalf("the killed App Server's disconnects %v; want none", disconnects())
	case !conflicted:
		t.Fatal("the restarted App Server's status never read connection_conflict")
	// The edge notes a request's arrival before the hub reads the database
	// clock, so the first 200 may arrive up to a moment before the lease end.
	case first < 1 || requests[first].status != http.StatusOK || !requests[first-1].at.Before(lease) || requests[first].at.Before(lease.Add(-time.Second)):
		t.Fatalf("the event requests after the restart %+v, the old lease ending %s; want 409 while the lease lasted, then 200", requests, lease)
	}
	record.observe("the killed App Server sent no disconnect; the restart's %d event requests up to %s, before the old lease ended at %s, were answered 409, its status read connection_conflict and never blocked, and the request at %s, after the lease, was answered 200 and it read online", first, requests[first-1].at.Format(time.RFC3339Nano), lease.Format(time.RFC3339Nano), requests[first].at.Format(time.RFC3339Nano))
}

// disconnectsOf gives the statuses of the disconnect requests of connection
// that the edge passes on.
func disconnectsOf(e *engine, connection string) func() []int {
	var mu sync.Mutex
	var statuses []int
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool {
			if r.Method != http.MethodPost || r.URL.Path != "/v1/client/disconnect" {
				return false
			}
			body, err := io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewReader(body))
			var request struct {
				ConnectionID string `json:"connection_id"`
			}
			return err == nil && json.Unmarshal(body, &request) == nil && request.ConnectionID == connection
		},
		after: func(response *http.Response) error {
			mu.Lock()
			defer mu.Unlock()
			statuses = append(statuses, response.StatusCode)
			return nil
		},
	})
	return func() []int {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(statuses)
	}
}

// connectionEnd reads when connection ended and why.
func connectionEnd(t *testing.T, e *engine, connection string) (time.Time, string) {
	t.Helper()
	var endedAt *time.Time
	var cause *string
	e.ownerTx(func(ctx context.Context, tx *dbconnect.Tx) error {
		return tx.QueryRow(ctx, `SELECT ended_at,end_cause FROM instance_connections WHERE workspace_id=$1 AND id=$2`, harnessWorkspace, connection).Scan(&endedAt, &cause)
	})
	if endedAt == nil || cause == nil {
		t.Fatalf("connection %s has not ended", connection)
	}
	return *endedAt, *cause
}

// closeListsHeldUploadsAndRejectsConflicts removes a computer while a
// background command would exit 20 seconds after its termination signal,
// the edge holds the content upload of one pending drive change, and the
// developer has committed the path of a second pending change. The closed
// report arrives within 30 seconds after the removal, plus the report round
// trip, with the held path alone in lost_changes; the second change's
// commit is rejected with conflict and writes no error; one
// drive_changes_lost_error lists the held path with paths_total 1 and a
// terminal retry status; the drive keeps both paths' versions from before
// and none from that machine.
func closeListsHeldUploadsAndRejectsConflicts(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	developer.upload(ctx, driveID, "held.txt", "old")
	developer.upload(ctx, driveID, "taken.txt", "base")
	m := onlineUserMachine(t, e, set, "machine")
	session, computers, rs := driveSession(t, e, "close", []sessionDrive{readWrite(driveID)}, m)
	holdUpload(e, driveID, "new")
	var mu sync.Mutex
	var commits, answers []string
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool {
			return r.Method == http.MethodPost && commitRoute(driveID).MatchString(r.URL.Path)
		},
		before: func(_ http.ResponseWriter, r *http.Request) bool {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				return false
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			mu.Lock()
			defer mu.Unlock()
			commits = append(commits, string(body))
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
			answers = append(answers, string(body))
			return nil
		},
	})
	rs.background(computers[0], `trap 'sleep 20; exit 0' TERM; while :; do sleep 1; done`)
	uploadOutside(e, m, session.ID, driveID, "printf new > held.txt && printf mine > taken.txt", 1)
	developer.upload(ctx, driveID, "taken.txt", "theirs")
	versions := func(path string) []string {
		var ids []string
		for _, version := range developer.listVersions(ctx, driveID, path) {
			ids = append(ids, version.VersionID)
		}
		return ids
	}
	before := map[string][]string{"held.txt": versions("held.txt"), "taken.txt": versions("taken.txt")}

	removed := time.Now()
	if status, object := developer.removeComputer(ctx, session.ID, computers[0]); status != http.StatusAccepted {
		t.Fatalf("removing the computer answered %d %+v; want 202", status, object)
	}
	_, report := e.waitReport(2*time.Minute, "closed report", func(report protocol.SessionStatus) bool {
		return report.Status == "closed" && report.SessionID == session.ID
	})
	took := time.Since(removed)
	lost := report.LostChanges.Value
	if took > 35*time.Second || len(lost) != 1 || lost[0].DriveID != driveID || !slices.Equal(lost[0].Paths, []string{"held.txt"}) || lost[0].PathsTotal != 1 || lost[0].RequestMessageID != report.RequestMessageID {
		t.Fatalf("the closed report came %s after the removal with lost_changes %+v; want held.txt alone under close %s within 30 seconds plus the round trip", took, lost, report.RequestMessageID)
	}
	record.observe("the closed report came %s after the removal, with lost_changes listing held.txt alone (paths_total 1)", took.Round(time.Millisecond))

	mu.Lock()
	sent, answered := slices.Clone(commits), slices.Clone(answers)
	mu.Unlock()
	if len(sent) != 1 || len(answered) != 1 || !strings.Contains(sent[0], `"taken.txt"`) || strings.Contains(sent[0], `"held.txt"`) || !strings.Contains(answered[0], `"status":"rejected","reason":"conflict"`) || strings.Contains(answered[0], `"accepted"`) {
		t.Fatalf("the drive's commits were %q answered %q; want one commit of taken.txt rejected with conflict", sent, answered)
	}
	errs := developer.sessionErrors(ctx, session.ID)
	if len(errs) != 1 || errs[0].Error.Type != "drive_changes_lost_error" || errs[0].Error.DriveID != driveID || !slices.Equal(errs[0].Error.Paths, []string{"held.txt"}) || errs[0].Error.PathsTotal != 1 || errs[0].Error.RetryStatus.Type != "terminal" {
		t.Fatalf("the Session's session.error events are %+v; want one drive_changes_lost_error of held.txt, paths_total 1, terminal", errs)
	}
	for path, ids := range before {
		if after := versions(path); !slices.Equal(after, ids) {
			t.Fatalf("%s has versions %q after the close; want %q from before it", path, after, ids)
		}
	}
	record.observe("the one commit, of taken.txt, was answered rejected with conflict; the one session.error is a drive_changes_lost_error of held.txt with paths_total 1 and retry_status terminal; held.txt and taken.txt keep their versions from before the close, none from the machine")
}

// removedDrivesLostChangesComeWithTheNextReady removes one of an idle
// Session's two read_write drives while the edge holds the content upload
// of its one pending change, and ends the connection by killing the App
// Server before Engine stores the session.drives report. No
// drive_changes_lost_error is written before the reconnect; the restarted
// CLI's ready lists the change's path under the session.drives message,
// and exactly one drive_changes_lost_error lists it for the binding's
// computer; the drive keeps its old version; neither the new preparation's
// report nor the report of the other drive's later removal lists the path.
func removedDrivesLostChangesComeWithTheNextReady(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	removedDrive, otherDrive := developer.createDrive(ctx), developer.createDrive(ctx)
	developer.upload(ctx, removedDrive, "f", "old")
	m := onlineUserMachine(t, e, set, "machine")
	session, computers, _ := driveSession(t, e, "drives", []sessionDrive{readWrite(removedDrive), readWrite(otherDrive)}, m)
	previous := *developer.readSessionComputer(ctx, session.ID, computers[0]).BindingID
	holdUpload(e, removedDrive, "new")
	withheld, stopWithholding := withholdReports(e, func(report protocol.SessionStatus) bool {
		return report.Status == "applied" && report.SessionID == session.ID
	})
	readies := captureReadies(e)
	if code, out := m.shell("sh", "-c", `printf new > "$1/f"`, "sh", m.mountPoint(session.ID, removedDrive)); code != 0 {
		t.Fatalf("writing f outside the CLI exited %d: %s", code, out)
	}
	if status := developer.removeDrive(ctx, session.ID, removedDrive); status != http.StatusNoContent {
		t.Fatalf("removing %s answered %d; want 204", removedDrive, status)
	}
	var report protocol.SessionStatus
	select {
	case report = <-withheld:
	case <-time.After(2 * time.Minute):
		t.Fatal("no session.drives report reached the edge within 2 minutes of the removal")
	}
	if lost := report.LostChanges.Value; len(lost) != 1 || lost[0].DriveID != removedDrive || !slices.Equal(lost[0].Paths, []string{"f"}) {
		t.Fatalf("the withheld session.drives report lists lost_changes %+v; want f of %s", lost, removedDrive)
	}
	if errs := developer.lostChangeErrors(ctx, session.ID); len(errs) != 0 {
		t.Fatalf("drive_changes_lost_error events %+v before the reconnect; want none", errs)
	}
	record.observe("the edge withheld the session.drives report answering %s, which lists f in lost_changes; no drive_changes_lost_error was written", report.RequestMessageID)

	if err := m.server.signal(syscall.SIGKILL); err != nil {
		t.Fatalf("killing the App Server: %v", err)
	}
	stopWithholding()
	m.server = m.serve()
	ready, _ := readies.wait(0, 2*time.Minute, func(protocol.Ready) bool { return true })
	if lost := ready.LostChanges.Value; len(lost) != 1 || lost[0].RequestMessageID != report.RequestMessageID || lost[0].DriveID != removedDrive || !slices.Equal(lost[0].Paths, []string{"f"}) || lost[0].PathsTotal != 1 {
		t.Fatalf("the restarted CLI's ready lists lost_changes %+v; want f of %s, paths_total 1, under %s", lost, removedDrive, report.RequestMessageID)
	}
	preparedAgain(e, session.ID, computers[0], previous)
	binding := *developer.readSessionComputer(ctx, session.ID, computers[0]).BindingID
	_, prepared := e.waitReport(time.Minute, "prepared report", func(r protocol.SessionStatus) bool {
		return r.Status == "prepared" && r.BindingID == binding
	})
	if prepared.LostChanges.Set {
		t.Fatalf("the new preparation's report lists lost_changes %+v; want none", prepared.LostChanges.Value)
	}
	var errs []sessionError
	for deadline := time.Now().Add(30 * time.Second); len(errs) == 0 && time.Now().Before(deadline); time.Sleep(time.Second) {
		errs = developer.lostChangeErrors(ctx, session.ID)
	}
	if len(errs) != 1 || errs[0].Error.DriveID != removedDrive || errs[0].Error.ComputerID != computers[0] || !slices.Equal(errs[0].Error.Paths, []string{"f"}) || errs[0].Error.PathsTotal != 1 {
		t.Fatalf("drive_changes_lost_error events %+v after the reconnect; want one of f for %s with paths_total 1", errs, computers[0])
	}
	record.observe("the restarted CLI's ready on connection %s lists f under %s; the computer was prepared again with binding %s, whose report lists no lost change; one drive_changes_lost_error of f for %s", ready.ConnectionID, report.RequestMessageID, binding, computers[0])

	if status := developer.removeDrive(ctx, session.ID, otherDrive); status != http.StatusNoContent {
		t.Fatalf("removing %s answered %d; want 204", otherDrive, status)
	}
	_, applied := e.waitReport(2*time.Minute, "session.drives report", func(r protocol.SessionStatus) bool {
		return r.Status == "applied" && r.BindingID == binding
	})
	if applied.LostChanges.Set && len(applied.LostChanges.Value) != 0 {
		t.Fatalf("the later session.drives report lists lost_changes %+v; want none", applied.LostChanges.Value)
	}
	if status, text := developer.readFile(ctx, removedDrive, "f"); status != http.StatusOK || text != "old" {
		t.Fatalf("f of %s reads %d %q; want the old version", removedDrive, status, text)
	}
	if versions := developer.listVersions(ctx, removedDrive, "f"); len(versions) != 1 {
		t.Fatalf("f has versions %+v; want only the developer's", versions)
	}
	if errs := developer.lostChangeErrors(ctx, session.ID); len(errs) != 1 {
		t.Fatalf("drive_changes_lost_error events %+v at the end; want the one", errs)
	}
	record.observe("the removal of %s was reported without lost_changes; f of %s keeps its old version alone and the Session has the one drive_changes_lost_error", otherDrive, removedDrive)
}

// holdUpload keeps the edge from passing on any content upload of content
// to driveID until the test ends.
func holdUpload(e *engine, driveID, content string) {
	route := regexp.MustCompile(`^/v1/client/drives/` + driveID + `/content/` + sha256Hex(content) + `$`)
	release := make(chan struct{})
	e.t.Cleanup(func() { close(release) })
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool { return r.Method == http.MethodPut && route.MatchString(r.URL.Path) },
		before: func(_ http.ResponseWriter, r *http.Request) bool {
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return false
		},
	})
}

// endConnection kills the computer-hub pod of the machine's stream and
// holds its reconnects until the instance reads offline; then runs between
// and releases them.
func endConnection(e *engine, m *userMachine, between func()) {
	e.t.Helper()
	hold := e.edge.holdEvents(m.instanceID)
	requests := e.edge.eventsRequests(m.instanceID)
	e.killHub(requests[len(requests)-1].backend)
	hold.waitArrived(e.t, time.Minute)
	m.waitOffline(e)
	between()
	e.edge.releaseEvents(m.instanceID)
}

// refuseHeartbeats answers every heartbeat of connectionID with 503, so
// that the CLI ends the connection once its last grant runs out.
func refuseHeartbeats(e *engine, connectionID string) {
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool {
			if r.Method != http.MethodPost || r.URL.Path != "/v1/client/heartbeat" {
				return false
			}
			body, err := io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewReader(body))
			return err == nil && bytes.Contains(body, []byte(connectionID))
		},
		before: func(w http.ResponseWriter, _ *http.Request) bool {
			w.WriteHeader(http.StatusServiceUnavailable)
			return true
		},
	})
}

// closeMessage is the close message ID the computer's binding holds, and
// the binding's kind.
func (e *engine) closeMessage(computerID string) (kind, closeID string) {
	e.t.Helper()
	e.ownerTx(func(ctx context.Context, tx *dbconnect.Tx) error {
		return tx.QueryRow(ctx, `SELECT b.kind,COALESCE(b.close_message_id,'') FROM computers c JOIN computer_bindings b ON b.workspace_id=c.workspace_id AND b.binding_id=c.binding_id WHERE c.workspace_id=$1 AND c.id=$2`, harnessWorkspace, computerID).Scan(&kind, &closeID)
	})
	return kind, closeID
}

// lostChangesOfACloseSurviveReconnects removes a computer while the
// edge holds the content upload of its one pending drive change, and ends
// the connection before Engine stores the closed report. No
// drive_changes_lost_error is written before the reconnect. The first new
// connection's ready lists the path under the close, and one
// drive_changes_lost_error is written; the edge withholds the ACKs of that
// connection's readies, so the CLI gives the connection up, and the
// following connection's ready
// lists the same entry and writes no second error; the late close reports
// closed without lost_changes and the computer closes. In the repeat, the
// connection ends 10 seconds into the final upload: no ready lists the
// close, and the late close reports closed with the path in lost_changes
// under the late close, and one drive_changes_lost_error is written. The
// repeat ends the connection by refusing its heartbeats, so that the CLI
// gives it up when its last grant runs out, 10 seconds after the removal.
func lostChangesOfACloseSurviveReconnects(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	m := onlineUserMachine(t, e, set, "machine")
	readies := captureReadies(e)
	pending := func(name string) (sessionObject, string, string) {
		driveID := developer.createDrive(ctx)
		developer.upload(ctx, driveID, "f", "old")
		session, computers, _ := driveSession(t, e, name, []sessionDrive{readWrite(driveID)}, m)
		holdUpload(e, driveID, "new")
		if code, out := m.shell("sh", "-c", `printf new > "$1/f"`, "sh", m.mountPoint(session.ID, driveID)); code != 0 {
			t.Fatalf("writing f outside the CLI exited %d: %s", code, out)
		}
		return session, computers[0], driveID
	}
	lists := func(ready protocol.Ready, closeID string) bool {
		return slices.ContainsFunc(ready.LostChanges.Value, func(l protocol.LostChange) bool { return l.RequestMessageID == closeID })
	}

	session, computerID, driveID := pending("withheld")
	withheld, stopWithholding := withholdReports(e, func(report protocol.SessionStatus) bool {
		return report.Status == "closed" && report.SessionID == session.ID
	})
	if status, object := developer.removeComputer(ctx, session.ID, computerID); status != http.StatusAccepted {
		t.Fatalf("removing the computer answered %d %+v; want 202", status, object)
	}
	var closed protocol.SessionStatus
	select {
	case closed = <-withheld:
	case <-time.After(2 * time.Minute):
		t.Fatal("no closed report reached the edge within 2 minutes of the removal")
	}
	closeID := closed.RequestMessageID
	seen := len(readies.all())
	endConnection(e, m, func() {
		if errs := developer.lostChangeErrors(ctx, session.ID); len(errs) != 0 {
			t.Fatalf("drive_changes_lost_error events %+v before the reconnect; want none", errs)
		}
		readies.holdNextConnection()
	})
	record.observe("the edge withheld the closed report of close %s and the connection ended; no drive_changes_lost_error before the reconnect", closeID)
	first, _ := readies.wait(seen, 2*time.Minute, func(protocol.Ready) bool { return true })
	stopWithholding()
	if lost := first.LostChanges.Value; len(lost) != 1 || lost[0].RequestMessageID != closeID || lost[0].DriveID != driveID || !slices.Equal(lost[0].Paths, []string{"f"}) || lost[0].PathsTotal != 1 {
		t.Fatalf("the first new connection's ready lists lost_changes %+v; want f of %s, paths_total 1, under %s", lost, driveID, closeID)
	}
	following, index := readies.wait(seen+1, 2*time.Minute, func(r protocol.Ready) bool { return r.ConnectionID != first.ConnectionID })
	sameEntry := func(a, b protocol.LostChange) bool {
		return a.RequestMessageID == b.RequestMessageID && a.DriveID == b.DriveID && slices.Equal(a.Paths, b.Paths) && a.PathsTotal == b.PathsTotal
	}
	if !slices.EqualFunc(following.LostChanges.Value, first.LostChanges.Value, sameEntry) {
		t.Fatalf("the following ready on %s lists lost_changes %+v; want a new connection with %+v", following.ConnectionID, following.LostChanges.Value, first.LostChanges.Value)
	}
	_, late := e.waitReport(2*time.Minute, "closed report of the late close", func(r protocol.SessionStatus) bool {
		return r.Status == "closed" && r.SessionID == session.ID
	})
	if late.LostChanges.Set && len(late.LostChanges.Value) != 0 {
		t.Fatalf("the late close's closed report lists lost_changes %+v; want none", late.LostChanges.Value)
	}
	developer.waitClosed(ctx, session.ID, computerID, 2*time.Minute)
	if errs := developer.lostChangeErrors(ctx, session.ID); len(errs) != 1 || !slices.Equal(errs[0].Error.Paths, []string{"f"}) || errs[0].Error.PathsTotal != 1 {
		t.Fatalf("drive_changes_lost_error events %+v; want one of f with paths_total 1", errs)
	}
	record.observe("connection %s's ready listed f under %s and its ACK was withheld; connection %s's ready listed the same entry; the late close %s reported closed without lost_changes; the computer closed with one drive_changes_lost_error", first.ConnectionID, closeID, following.ConnectionID, late.RequestMessageID)

	afterFollowing := index + 1
	session, computerID, driveID = pending("ended")
	old := controllerConnection(t, e, m.instanceID)
	refuseHeartbeats(e, old)
	time.Sleep(2 * time.Second)
	expires := e.controller().readInstance(ctx, m.instanceID).Connection.LeaseExpiresAt
	time.Sleep(time.Until(expires.Add(-10 * time.Second)))
	if status, object := developer.removeComputer(ctx, session.ID, computerID); status != http.StatusAccepted {
		t.Fatalf("removing the computer answered %d %+v; want 202", status, object)
	}
	removed := time.Now()
	next, _ := readies.wait(afterFollowing, 3*time.Minute, func(r protocol.Ready) bool { return r.ConnectionID != old })
	record.observe("with connection %s's heartbeats refused, %s was removed %s before its lease ran out at %s; connection %s followed", old, computerID, expires.Sub(removed).Round(time.Millisecond), expires.Format(time.RFC3339Nano), next.ConnectionID)
	_, late = e.waitReport(2*time.Minute, "closed report of the late close", func(r protocol.SessionStatus) bool {
		return r.Status == "closed" && r.SessionID == session.ID
	})
	kind, lateID := e.closeMessage(computerID)
	if lost := late.LostChanges.Value; kind != "late_close" || late.RequestMessageID != lateID || len(lost) != 1 || lost[0].RequestMessageID != lateID || lost[0].DriveID != driveID || !slices.Equal(lost[0].Paths, []string{"f"}) {
		t.Fatalf("the closed report answering %s lists lost_changes %+v; want f of %s under the late close %s (binding %s)", late.RequestMessageID, lost, driveID, lateID, kind)
	}
	developer.waitClosed(ctx, session.ID, computerID, 2*time.Minute)
	for _, ready := range readies.all()[afterFollowing:] {
		if lists(ready, closeID) || slices.ContainsFunc(ready.LostChanges.Value, func(l protocol.LostChange) bool { return l.DriveID == driveID }) {
			t.Fatalf("ready of %s lists lost_changes %+v; want neither the first close's entry nor one of %s", ready.ConnectionID, ready.LostChanges.Value, driveID)
		}
	}
	if errs := developer.lostChangeErrors(ctx, session.ID); len(errs) != 1 || !slices.Equal(errs[0].Error.Paths, []string{"f"}) {
		t.Fatalf("drive_changes_lost_error events %+v of the repeat; want one of f", errs)
	}
	record.observe("after the connection ended 10 seconds into the final upload, no ready listed the close; the late close %s reported closed with f in lost_changes under it, with one drive_changes_lost_error; no ready after the first close's following one lists its entry", lateID)
}
