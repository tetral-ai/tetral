package joint

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// TestComputerManagement runs the scenarios of instances, launchers,
// credentials, wake requests and computer removal on one Engine with two
// computer-hub pods.
func TestComputerManagement(t *testing.T) {
	scenarioTable(t, engineOptions{hubPods: 2}, needs{},
		tableRow("launchers_come_online", personalAndProviderLaunchersComeOnline).budget(95, 130),
		tableRow("revocation", revocationEndsTheLiveConnectionAtOnce).budget(75, 75),
		tableRow("new_request_starts_a_machine", controllerStartsAMachineForANewRequest).budget(25, 30),
		tableRow("stopped_instance_wakes", stoppedCloudInstanceWakesWithItsIdentity).budget(70, 75),
		tableRow("failed_wake_request", failedWakeRequestLeavesTheInstanceOffline).budget(70, 75),
		tableRow("removing_one_computer", removingOneComputerLeavesTheOthers).budget(25, 40),
		tableRow("removed_while_offline", computerRemovedWhileOfflineClosesOnReconnect).budget(100, 110),
		tableRow("archived_cloud_instances", archivedCloudInstancesTurnUnusedOnceClosed).budget(150, 160),
		tableRow("removal_during_work", removalStartsNothingNewAndNeverClosesABlockedCleanup).budget(120, 160),
	)
}

// personalAndProviderLaunchersComeOnline runs a product app's launch
// (configure, then server run) and a provider's noninteractive launch (the
// same configure, then server run --idle-exit) of two user instances. Both
// come online while the workspace has no Session. Then, before any recovery
// step, the instance read tells the two delivery outcomes apart: the
// provider instance's replacement credential, installed through configure
// and a restart, is its connection's credential, so a delivery whose
// acknowledgement the Controller lost worked; the personal instance's
// further credential, never delivered, is not in use. Last, a call to a
// Session's cloud computer records a request, the provider's launch
// configures the bootstrap of its claim, and the cloud instance comes online,
// prepares the computer and runs the waiting call.
func personalAndProviderLaunchersComeOnline(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	record.boundary("this scenario observes only credential delivery through configure and a restart; it does not exercise delivery over a live connection (delivery: stream)")
	record.boundary("the idle exit itself does not happen: the run ends before 15 minutes without a command")
	controller, developer := e.controller(), e.developer()
	ctx := t.Context()

	launches := []struct {
		name, app string
		args      []string
	}{
		{"personal", "personal-app", nil},
		{"provider", "provider-launcher", []string{"--idle-exit", "15m"}},
	}
	machines := map[string]*machine{}
	servers := map[string]*cliProcess{}
	registrations := map[string]registration{}
	for _, launch := range launches {
		registered := controller.createUserInstance(ctx, launch.name)
		m := set.add(launch.name, launch.app)
		m.configure(bootstrap(e.serverURL, registered.instance.ID, registered.credential))
		servers[launch.name] = m.serve(launch.args...)
		requireOnline(t, controller, m, registered.instance.ID, registered.credential.ID)
		machines[launch.name], registrations[launch.name] = m, registered
	}
	if sessions := developer.listSessions(ctx); len(sessions) != 0 {
		t.Fatalf("the workspace lists %d Sessions; want none", len(sessions))
	}
	record.observe("both instances online while GET /v1/sessions lists no Session")

	provider, replaced := machines["provider"], registrations["provider"]
	delivered := controller.issueResponseCredential(ctx, replaced.instance.ID)
	if exit := servers["provider"].stop(); exit.exitCode != 0 {
		t.Fatalf("server run on provider exited %d: %s", exit.exitCode, exit.stdout)
	}
	provider.configure(bootstrap(e.serverURL, replaced.instance.ID, delivered))
	restarted := time.Now()
	provider.serve("--idle-exit", "15m")
	requireOnline(t, controller, provider, replaced.instance.ID, delivered.ID)
	record.observe("the provider launcher installed replacement credential %s with configure and restarted; online again after %s, the stop having ended its previous connection with a disconnect", delivered.ID, time.Since(restarted).Round(time.Second))
	requireOnlyInUse(t, controller, replaced.instance.ID, delivered.ID, replaced.credential.ID)
	record.observe("provider instance read: connection.credential_id is the delivered %s, which alone is in_use; the first %s is not in use; none is revoked", delivered.ID, replaced.credential.ID)

	personal := registrations["personal"]
	undelivered := controller.issueResponseCredential(ctx, personal.instance.ID)
	requireOnlyInUse(t, controller, personal.instance.ID, personal.credential.ID, undelivered.ID)
	record.observe("personal instance read: connection.credential_id is still %s, which alone is in_use; the undelivered %s is not in use; none is revoked", personal.credential.ID, undelivered.ID)

	cloud := set.add("provider-claim", "provider-launcher")
	s := developer.createCloudSession(ctx, "provider-claim", cloud.directory("workspace"))
	rs := e.startRuntime().bind(s.sessionID)
	call, request := waitingCall(t, e, rs, s, "pwd")
	claimed := controller.claimRequest(ctx, request.ID, "provider-worker")
	cloudInstance, cloudCredential := claimed.bootstrapFields(t)
	cloud.configure(claimed.Bootstrap)
	cloud.serve("--idle-exit", "15m")
	requireCloudOnline(t, e, cloud, cloudInstance, cloudCredential, s)
	rs.requireCompleted(call, time.Minute, cloud.directory("workspace"))
	record.observe("the provider launcher configured the bootstrap of a claim for cloud instance %s, came online, prepared the claimed computer's Session and ran the waiting call in its working directory", cloudInstance)
}

// requireOnlyInUse checks that instanceID's connection is online with
// credential inUse, that the credential list marks it alone in_use and
// unused not, and that no credential of the instance is revoked.
func requireOnlyInUse(t *testing.T, controller *controllerClient, instanceID, inUse, unused string) {
	t.Helper()
	ctx := t.Context()
	object := controller.readInstance(ctx, instanceID)
	if object.Connection == nil || object.Connection.State != "online" || object.Connection.CredentialID != inUse {
		t.Fatalf("instance %s read: connection %+v; want online with credential %s", instanceID, object.Connection, inUse)
	}
	marked := map[string]bool{}
	for _, credential := range controller.listCredentials(ctx, instanceID) {
		marked[credential.ID] = credential.InUse
		if credential.RevokedAt != nil {
			t.Fatalf("credential %s is revoked; no credential of this test is", credential.ID)
		}
	}
	if _, listed := marked[unused]; !listed || !marked[inUse] || marked[unused] || len(marked) != 2 {
		t.Fatalf("instance %s in_use by credential %v; want %s alone of %s and %s", instanceID, marked, inUse, inUse, unused)
	}
}

// revocationEndsTheLiveConnectionAtOnce revokes a CLI's credential just
// before its next heartbeat is due, while a command's admission to a
// Session's user computer on that instance races it. The instance read shows
// the connection offline as soon as the revocation returns; no heartbeat is
// accepted and no lease granted after the revocation; no instruction is
// written to the connection after its end, and the racing call ends not
// executed with the computer offline or, when its instruction was written
// before, with an unknown outcome; the CLI blocks with credential_revoked
// and opens no other connection.
func revocationEndsTheLiveConnectionAtOnce(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	controller := e.controller()
	ctx := t.Context()

	registered := controller.createUserInstance(ctx, "revoked")
	m := set.add("revoked", "revoked-app")
	m.configure(bootstrap(e.serverURL, registered.instance.ID, registered.credential))
	m.serve()
	requireOnline(t, controller, m, registered.instance.ID, registered.credential.ID)
	session, computerID := userComputerSession(t, e, m, registered.instance.ID, "revoked")
	rs := e.startRuntime().bind(session.ID)
	use := rs.use(protocol.ToolExecCommand, `{"cmd":"sleep 30"}`, "machine_tool")
	beat := controller.waitInstance(ctx, registered.instance.ID, 45*time.Second, "a heartbeat accepted after the preparation", func(object instanceObject) bool {
		return object.Connection != nil && object.Connection.LastHeartbeatAt != nil && time.Until(object.Connection.LastHeartbeatAt.Add(15*time.Second)) > time.Second
	})
	connection := beat.Connection
	due := connection.LastHeartbeatAt.Add(15 * time.Second)
	time.Sleep(time.Until(due.Add(-20 * time.Millisecond)))
	admitted := make(chan error, 1)
	go func() {
		response, err := rs.r.client.AdmitToolCall(ctx, &bridgev1.AdmitToolCallRequest{Scope: rs.scope, ToolUseEventId: use, ComputerId: computerID,
			Tool: protocol.ToolExecCommand, InputJson: `{"cmd":"sleep 30"}`, SandboxPolicyJson: scenarioPolicy})
		if err == nil && response.GetAdmitted() == nil {
			err = fmt.Errorf("admission answered %v; want admitted", response)
		}
		admitted <- err
	}()
	controller.revokeCredential(ctx, registered.instance.ID, registered.credential.ID)
	if err := <-admitted; err != nil {
		t.Fatalf("the admission racing the revocation: %v", err)
	}

	ended := controller.readInstance(ctx, registered.instance.ID)
	if ended.Connection == nil || ended.Connection.ID != connection.ID || ended.Connection.State != "offline" {
		t.Fatalf("instance read right after the revocation: connection %+v; want %s offline", ended.Connection, connection.ID)
	}
	var revokedAt time.Time
	for _, credential := range controller.listCredentials(ctx, registered.instance.ID) {
		if credential.ID == registered.credential.ID && credential.RevokedAt != nil {
			revokedAt = *credential.RevokedAt
		}
	}
	if revokedAt.IsZero() {
		t.Fatal("the credential list shows no revoked_at for the revoked credential")
	}
	if ended.Connection.LastHeartbeatAt == nil || ended.Connection.LastHeartbeatAt.After(revokedAt) {
		t.Fatalf("last_heartbeat_at %v is after revoked_at %s", ended.Connection.LastHeartbeatAt, revokedAt)
	}
	record.observe("revoked at %s, %s after the last accepted heartbeat at %s; the instance read right after DELETE showed connection %s offline", revokedAt.Format(time.RFC3339Nano), revokedAt.Sub(*connection.LastHeartbeatAt), connection.LastHeartbeatAt.Format(time.RFC3339Nano), connection.ID)

	settled := rs.await(use, time.Minute)
	if !(settled.Settlement == "not_executed" && settled.Code == "computer_offline") && !(settled.Settlement == "unknown_outcome" && settled.Message == "computer offline, result unknown") {
		t.Fatalf("the racing call settled %+v; want not executed with the computer offline or an unknown outcome", settled)
	}
	var written, late int
	e.ownerTx(func(ctx context.Context, tx *dbconnect.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FILTER (WHERE i.first_written_at IS NOT NULL), count(*) FILTER (WHERE i.first_written_at >= c.ended_at)
			FROM connection_instructions i JOIN instance_connections c ON c.workspace_id=i.workspace_id AND c.id=i.connection_id
			WHERE c.id=$1 AND i.type='tool.execute'`, connection.ID).Scan(&written, &late)
	})
	if late != 0 {
		t.Fatalf("%d tool.execute instructions were first written to connection %s at or after its end", late, connection.ID)
	}
	record.observe("the racing call settled %s %s %q; %d tool.execute instructions were written to connection %s, none at or after its end", settled.Settlement, settled.Code, settled.Message, written, connection.ID)

	revoked := time.Now()
	status := m.waitStatus(10*time.Second, "blocked with credential_revoked", func(status map[string]any) bool {
		return statusField(status, "connection_state") == "blocked" && lastErrorCode(status) == "credential_revoked"
	})
	record.observe("the CLI's status was blocked with last_error credential_revoked %s after the revocation returned: %s", time.Since(revoked).Round(time.Millisecond), mustJSON(t, status["last_error"]))

	time.Sleep(time.Until(due.Add(20 * time.Second)))
	later := controller.readInstance(ctx, registered.instance.ID)
	if later.Connection == nil || later.Connection.ID != connection.ID || later.Connection.State != "offline" ||
		!later.Connection.LeaseExpiresAt.Equal(ended.Connection.LeaseExpiresAt) || !later.Connection.LastHeartbeatAt.Equal(*ended.Connection.LastHeartbeatAt) {
		t.Fatalf("instance read 20s after the due heartbeat: connection %+v; want %+v unchanged", later.Connection, ended.Connection)
	}
	if status := m.status(); statusField(status, "connection_state") != "blocked" {
		t.Fatalf("the CLI left blocked: %v", status)
	}
	record.observe("20s after the due heartbeat: the same connection %s offline with lease_expires_at %s and last_heartbeat_at unchanged; no other connection; the CLI still blocked", connection.ID, later.Connection.LeaseExpiresAt.Format(time.RFC3339Nano))
}

// requireOnline waits until the machine is online with credentialID, as
// waitOnline does, and checks that the instance has no computer and no
// binding.
func requireOnline(t *testing.T, controller *controllerClient, m *machine, instanceID, credentialID string) {
	t.Helper()
	object := waitOnline(t, controller, m, instanceID, credentialID)
	if object.ComputerCount != 0 || object.BindingCount != 0 {
		t.Fatalf("instance read computer_count %d binding_count %d; want 0 and 0", object.ComputerCount, object.BindingCount)
	}
	m.record.observe("%s: no computer and no binding on instance %s", m.name, instanceID)
}

// waitOnline waits until the machine's status and the instance read both
// show the connection online with credentialID, checks what the read reports
// of the connection and returns the read.
func waitOnline(t *testing.T, controller *controllerClient, m *machine, instanceID, credentialID string) instanceObject {
	t.Helper()
	status := m.waitStatus(2*time.Minute, "connection_state online", func(status map[string]any) bool {
		return statusField(status, "connection_state") == "online"
	})
	if statusField(status, "instance_id") != instanceID || statusField(status, "credential_id") != credentialID {
		t.Fatalf("%s status names instance %s and credential %s; want %s and %s", m.name, status["instance_id"], status["credential_id"], instanceID, credentialID)
	}
	object := controller.waitInstance(t.Context(), instanceID, 30*time.Second, "connection online", func(object instanceObject) bool {
		return object.Connection != nil && object.Connection.State == "online"
	})
	switch {
	case object.Connection.ID != statusField(status, "connection_id"):
		t.Fatalf("instance read connection %s; the CLI's status names %s", object.Connection.ID, status["connection_id"])
	case object.Connection.CredentialID != credentialID:
		t.Fatalf("instance read connection credential %s; want %s", object.Connection.CredentialID, credentialID)
	case object.CLIVersion == nil || *object.CLIVersion != m.cliVersion:
		t.Fatalf("instance read cli_version %v; the machine's tetral is %s", object.CLIVersion, m.cliVersion)
	case object.Connection.ReadyAt == nil:
		t.Fatalf("instance read connection %+v has no ready_at", object.Connection)
	}
	m.record.observe("%s: status online with connection %s; instance read online with credential %s, cli_version %s, ready_at %s", m.name, object.Connection.ID, credentialID, *object.CLIVersion, object.Connection.ReadyAt.Format(time.RFC3339Nano))
	return object
}

// waitingCall admits command cmd to the Session's cloud computer, which no
// online machine serves: the admission records the computer's open request,
// then the one pending request of the computer's Environment, and the call
// waits for it.
func waitingCall(t *testing.T, e *engine, rs *runtimeSession, s cloudSession, cmd string) (toolCall, requestObject) {
	t.Helper()
	call := rs.exec(s.computerID, cmd)
	pending := e.controller().listRequests(t.Context(), s.environmentID)
	if len(pending) != 1 {
		t.Fatalf("pending requests of %s after a call to %s: %+v; want one", s.environmentID, s.computerID, pending)
	}
	e.record.observe("call %s to %s waits for %s request %s, the one pending request of %s", call.use, s.computerID, pending[0].Kind, pending[0].ID, s.environmentID)
	return call, pending[0]
}

// readRequest reads a request in-process; no route serves a request that
// has left pending.
func (e *engine) readRequest(requestID string) computer.Request {
	e.t.Helper()
	var request computer.Request
	e.ownerTx(func(ctx context.Context, tx *dbconnect.Tx) error {
		var err error
		request, err = computer.ReadRequest(ctx, tx, workspace.ID(harnessWorkspace), requestID)
		return err
	})
	return request
}

// requireCloudOnline waits until the machine's status and the instance read
// both show the connection online with credentialID, and checks that the
// instance is the active cloud instance of the Session's computer, which
// holds it as current. The ready prepares that computer on the connection:
// it then waits until the computer is ready there in its working directory
// and the instance counts that one prepared binding.
func requireCloudOnline(t *testing.T, e *engine, m *machine, instanceID, credentialID string, s cloudSession) instanceObject {
	t.Helper()
	controller := e.controller()
	status := m.waitStatus(2*time.Minute, "connection_state online", func(status map[string]any) bool {
		return statusField(status, "connection_state") == "online"
	})
	if statusField(status, "instance_id") != instanceID || statusField(status, "credential_id") != credentialID {
		t.Fatalf("%s status names instance %s and credential %s; want %s and %s", m.name, status["instance_id"], status["credential_id"], instanceID, credentialID)
	}
	object := controller.waitInstance(t.Context(), instanceID, 30*time.Second, "connection online", func(object instanceObject) bool {
		return object.Connection != nil && object.Connection.State == "online"
	})
	switch {
	case object.Connection.ID != statusField(status, "connection_id") || object.Connection.CredentialID != credentialID:
		t.Fatalf("instance read connection %+v; the CLI's status names connection %s with credential %s", object.Connection, status["connection_id"], credentialID)
	case object.Kind != "cloud" || object.ComputerID == nil || *object.ComputerID != s.computerID || object.RegistrationState != "active":
		t.Fatalf("instance read kind %s computer_id %v registration %s; want an active cloud instance of %s", object.Kind, object.ComputerID, object.RegistrationState, s.computerID)
	case object.ComputerCount != 1 || object.Unused:
		t.Fatalf("instance read computer_count %d unused %v; want 1 and false", object.ComputerCount, object.Unused)
	}
	connectionID := object.Connection.ID
	prepared := e.developer().waitComputer(t.Context(), s.sessionID, s.computerID, time.Minute, "a preparation reported on "+connectionID, func(c computerObject) bool {
		return c.ConnectionID != nil && *c.ConnectionID == connectionID && c.ExecutionState != "preparing"
	})
	if prepared.ExecutionState != "ready" || prepared.CWD == nil || *prepared.CWD != prepared.WorkingDirectory {
		t.Fatalf("computer %s after its preparation on %s: %+v; want ready in its working directory", s.computerID, connectionID, prepared)
	}
	object = controller.waitInstance(t.Context(), instanceID, 30*time.Second, "binding_count 1", func(object instanceObject) bool { return object.BindingCount == 1 })
	m.record.observe("%s: status online with connection %s; instance read online with credential %s, kind cloud, computer_id %s, computer_count 1, not unused; the computer ready on that connection with cwd %s, and binding_count 1", m.name, connectionID, credentialID, s.computerID, *prepared.CWD)
	return object
}

// nextRequestEvent is the next event of a request stream.
func nextRequestEvent(t *testing.T, events <-chan requestEvent) requestEvent {
	t.Helper()
	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("the request stream ended")
		}
		return event
	case <-time.After(10 * time.Second):
		t.Fatal("no request event within 10 seconds")
	}
	return requestEvent{}
}

// startClaimedMachine makes a call to the Session's cloud computer, claims
// the new request it records and starts a provider launch with the claim's
// bootstrap on m; it returns the running server and the claim's instance
// once online, prepared and done with the call.
func startClaimedMachine(t *testing.T, e *engine, rs *runtimeSession, m *machine, s cloudSession) (*cliProcess, string) {
	t.Helper()
	call, request := waitingCall(t, e, rs, s, "pwd")
	claimed := e.controller().claimRequest(t.Context(), request.ID, "worker-"+m.name)
	instanceID, credentialID := claimed.bootstrapFields(t)
	m.configure(claimed.Bootstrap)
	server := m.serve("--idle-exit", "15m")
	requireCloudOnline(t, e, m, instanceID, credentialID, s)
	rs.requireCompleted(call, time.Minute, m.directory("workspace"))
	return server, instanceID
}

// stopMachine stops a machine's server and waits until its instance reads
// offline.
func stopMachine(t *testing.T, controller *controllerClient, server *cliProcess, instanceID string) {
	t.Helper()
	if exit := server.stop(); exit.exitCode != 0 {
		t.Fatalf("server run on %s exited %d: %s", server.machine.name, exit.exitCode, exit.stdout)
	}
	controller.waitInstance(t.Context(), instanceID, 90*time.Second, "connection offline", func(object instanceObject) bool {
		return object.Connection != nil && object.Connection.State == "offline"
	})
}

// controllerStartsAMachineForANewRequest calls a Session's cloud
// computer, which has no instance: the call records a new request and waits.
// A Controller reads the request from the Environment's stream and list,
// claims it and starts a machine with the claim's bootstrap. The machine
// comes online as the cloud instance of that computer, the request is
// fulfilled, the instance is the computer's current instance and the waiting
// call completes there.
func controllerStartsAMachineForANewRequest(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	controller, developer := e.controller(), e.developer()
	ctx := t.Context()

	m := set.add("cloud", "provider-launcher")
	s := developer.createCloudSession(ctx, "new-request", m.directory("workspace"))
	events := controller.openRequestStream(ctx, s.environmentID)
	rs := e.startRuntime().bind(s.sessionID)
	call, request := waitingCall(t, e, rs, s, "pwd")
	if created := nextRequestEvent(t, events); created.name != "created" || created.request.ID != request.ID || request.Kind != "new" || request.State != "pending" {
		t.Fatalf("request stream event %+v for listed request %+v; want created of a pending new request", created, request)
	}
	record.observe("the stream of %s delivered created for new request %s", s.environmentID, request.ID)

	claimed := controller.claimRequest(ctx, request.ID, "worker-1")
	instanceID, credentialID := claimed.bootstrapFields(t)
	if claimed.Request.State != "claimed" || claimed.Request.InstanceID == nil || *claimed.Request.InstanceID != instanceID || claimed.WorkingDirectory == nil {
		t.Fatalf("claim result %+v; want the claimed request of instance %s with a working directory", claimed, instanceID)
	}
	if event := nextRequestEvent(t, events); event.name != "claimed" || event.request.ID != request.ID {
		t.Fatalf("request stream event %+v; want claimed of %s", event, request.ID)
	}
	m.configure(claimed.Bootstrap)
	m.serve("--idle-exit", "15m")
	requireCloudOnline(t, e, m, instanceID, credentialID, s)
	rs.requireCompleted(call, time.Minute, m.directory("workspace"))

	fulfilled := e.readRequest(request.ID)
	current := developer.readComputer(ctx, s).CurrentInstanceID
	if fulfilled.State != computer.RequestFulfilled || current == nil || *current != instanceID {
		t.Fatalf("request %s %s, computer current_instance_id %v; want fulfilled and %s", request.ID, fulfilled.State, current, instanceID)
	}
	record.observe("request %s fulfilled by the ready of %s, which the computer %s reads as current_instance_id; working_directory %s; the waiting call ran there", request.ID, instanceID, s.computerID, *claimed.WorkingDirectory)
}

// stoppedCloudInstanceWakesWithItsIdentity stops a cloud instance's
// machine and calls its computer: the call records a wake request for that
// instance and waits. A Controller claims it and restarts the machine with
// the claim's bootstrap; the machine comes online as the same instance under
// the claim's credential, whose first use revokes the instance's earlier
// credential, the request is fulfilled and the waiting call completes.
func stoppedCloudInstanceWakesWithItsIdentity(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	record.boundary("the machine is stopped with SIGTERM, not by its idle exit")
	controller, developer := e.controller(), e.developer()
	ctx := t.Context()

	cloud := set.add("cloud", "provider-launcher")
	s := developer.createCloudSession(ctx, "wake-request", cloud.directory("workspace"))
	rs := e.startRuntime().bind(s.sessionID)
	server, instanceID := startClaimedMachine(t, e, rs, cloud, s)
	first := controller.readInstance(ctx, instanceID).Connection.CredentialID
	stopMachine(t, controller, server, instanceID)
	record.observe("the machine of cloud instance %s stopped; the instance reads offline", instanceID)

	call, wake := waitingCall(t, e, rs, s, "pwd")
	if wake.Kind != "wake" || wake.InstanceID == nil || *wake.InstanceID != instanceID {
		t.Fatalf("request after the stop %+v; want a wake request for %s", wake, instanceID)
	}
	claimed := controller.claimRequest(ctx, wake.ID, "worker-wake")
	wakeInstance, wakeCredential := claimed.bootstrapFields(t)
	if wakeInstance != instanceID || claimed.WorkingDirectory != nil {
		t.Fatalf("wake claim result %+v; want a bootstrap for %s without a working directory", claimed, instanceID)
	}
	cloud.configure(claimed.Bootstrap)
	cloud.serve("--idle-exit", "15m")
	requireCloudOnline(t, e, cloud, instanceID, wakeCredential, s)
	rs.requireCompleted(call, time.Minute, cloud.directory("workspace"))

	revoked := false
	for _, credential := range controller.listCredentials(ctx, instanceID) {
		revoked = revoked || credential.ID == first && credential.RevokedAt != nil
	}
	if !revoked || e.readRequest(wake.ID).State != computer.RequestFulfilled {
		t.Fatalf("after the restart: earlier credential %s revoked %v, wake request %s; want revoked and fulfilled", first, revoked, e.readRequest(wake.ID).State)
	}
	record.observe("wake request %s claimed and fulfilled: the machine restarted with the claim's credential %s as the same instance %s, whose first use revoked the earlier %s, and ran the waiting call", wake.ID, wakeCredential, instanceID, first)
}

// failedWakeRequestLeavesTheInstanceOffline stops a cloud instance's
// machine and calls its computer; a Controller claims the wake request the
// call records and fails it. The next call records another wake request,
// which expires in the cleanup process's next pass. Each waiting call ends
// not executed with the machine-not-started text. After each, the computer
// keeps the instance as current, the instance stays offline and not unused,
// the Session gets no machine message, no session.computer_replaced and no
// session.error, and the next call records a new wake request for the same
// instance; the failed claim's credential is revoked.
func failedWakeRequestLeavesTheInstanceOffline(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	record.boundary("the wake request expires because the cleanup pass runs with TETRAL_CLEANUP_REQUEST_PENDING_EXPIRY=1s, not after the default 10 minutes")
	controller, developer := e.controller(), e.developer()
	ctx := t.Context()

	cloud := set.add("cloud", "provider-launcher")
	s := developer.createCloudSession(ctx, "failed-wake", cloud.directory("workspace"))
	rs := e.startRuntime().bind(s.sessionID)
	server, instanceID := startClaimedMachine(t, e, rs, cloud, s)
	stopMachine(t, controller, server, instanceID)

	call, wake := waitingCall(t, e, rs, s, "pwd")
	requireWake(t, wake, instanceID, "")
	claimed := controller.claimRequest(ctx, wake.ID, "worker-wake")
	_, claimCredential := claimed.bootstrapFields(t)
	if failed := controller.failRequest(ctx, wake.ID); failed.State != "failed" {
		t.Fatalf("fail answered %+v; want failed", failed)
	}
	rs.requireNotExecuted(call, time.Minute, "machine_not_started", "The computer's machine could not be started.")
	revoked := false
	for _, credential := range controller.listCredentials(ctx, instanceID) {
		revoked = revoked || credential.ID == claimCredential && credential.RevokedAt != nil
	}
	if !revoked {
		t.Fatalf("the failed claim's credential %s is not revoked", claimCredential)
	}
	requireStillOffline(t, e, s, instanceID)
	record.observe("wake request %s claimed and failed: the waiting call ended not executed with the machine-not-started text; claim credential %s revoked", wake.ID, claimCredential)

	call, expiring := waitingCall(t, e, rs, s, "pwd")
	requireWake(t, expiring, instanceID, wake.ID)
	time.Sleep(2 * time.Second)
	e.runCleanupPhases(map[string]string{"TETRAL_CLEANUP_REQUEST_PENDING_EXPIRY": "1s"})
	rs.requireNotExecuted(call, time.Minute, "machine_not_started", "The computer's machine could not be started.")
	if state := e.readRequest(expiring.ID).State; state != computer.RequestExpired {
		t.Fatalf("wake request %s after the cleanup pass is %s; want expired", expiring.ID, state)
	}
	requireStillOffline(t, e, s, instanceID)
	record.observe("wake request %s expired in the cleanup pass: the waiting call ended not executed with the machine-not-started text", expiring.ID)

	_, next := waitingCall(t, e, rs, s, "pwd")
	requireWake(t, next, instanceID, expiring.ID)
}

// requireWake checks that request is a wake request of instanceID other
// than previous.
func requireWake(t *testing.T, request requestObject, instanceID, previous string) {
	t.Helper()
	if request.ID == previous || request.Kind != "wake" || request.InstanceID == nil || *request.InstanceID != instanceID {
		t.Fatalf("request %+v; want a new wake request for %s", request, instanceID)
	}
}

// requireStillOffline checks that the Session's computer keeps instanceID as
// current, that the instance is offline and not unused, and that the
// Session has no machine message, session.computer_replaced or
// session.error.
func requireStillOffline(t *testing.T, e *engine, s cloudSession, instanceID string) {
	t.Helper()
	ctx := t.Context()
	object := e.controller().readInstance(ctx, instanceID)
	current := e.developer().readComputer(ctx, s).CurrentInstanceID
	switch {
	case current == nil || *current != instanceID:
		t.Fatalf("computer current_instance_id %v; want %s", current, instanceID)
	case object.Connection == nil || object.Connection.State != "offline" || object.Unused:
		t.Fatalf("instance %s: connection %+v, unused %v; want offline and not unused", instanceID, object.Connection, object.Unused)
	}
	var messages, notices int
	e.ownerTx(func(ctx context.Context, tx *dbconnect.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM session_machine_messages WHERE session_id=$1`, s.sessionID).Scan(&messages); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type IN ('session.computer_replaced','session.error')`, s.sessionID).Scan(&notices)
	})
	if messages != 0 || notices != 0 {
		t.Fatalf("the Session got %d machine messages and %d replacement or error events; want none", messages, notices)
	}
	e.record.observe("computer %s keeps current_instance_id %s, offline and not unused; no machine message, session.computer_replaced or session.error in the Session's tables", s.computerID, instanceID)
}

// removingOneComputerLeavesTheOthers gives Sessions S1 and S2 each a user
// computer on instances I1 and I2 with their own working directories. S1's
// two calls run on its two computers, each in its directory; removing S1's
// computer on I1 closes it and leaves S1's computer on I2 and both of S2's
// ready on the same bindings, running commands.
func removingOneComputerLeavesTheOthers(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	first, second := onlineUserMachine(t, e, set, "first"), onlineUserMachine(t, e, set, "second")
	s1, c1, r1 := driveSession(t, e, "s1", nil, first, second)
	s2, c2, r2 := driveSession(t, e, "s2", nil, first, second)
	machines := []*userMachine{first, second}
	for i, m := range machines {
		if out, _ := r1.run(c1[i], "pwd", time.Minute); out != m.directory("s1") {
			t.Fatalf("S1's call to its computer on %s ran in %q; want %s", m.name, out, m.directory("s1"))
		}
	}
	kept := map[string]computerObject{}
	for _, c := range []struct{ session, computer string }{{s1.ID, c1[1]}, {s2.ID, c2[0]}, {s2.ID, c2[1]}} {
		kept[c.computer] = developer.readSessionComputer(ctx, c.session, c.computer)
	}

	status, removed := developer.removeComputer(ctx, s1.ID, c1[0])
	if status != http.StatusAccepted || removed.AssociationState != "detached" {
		t.Fatalf("removing S1's computer on %s answered %d %+v; want 202 detached", first.name, status, removed)
	}
	developer.waitClosed(ctx, s1.ID, c1[0], time.Minute)
	record.observe("S1's computer %s on %s was removed (202, %s) and closed", c1[0], first.name, removed.ExecutionState)
	runs := []struct {
		rs       *runtimeSession
		session  string
		computer string
		dir      string
	}{{r1, s1.ID, c1[1], second.directory("s1")}, {r2, s2.ID, c2[0], first.directory("s2")}, {r2, s2.ID, c2[1], second.directory("s2")}}
	for _, run := range runs {
		if out, _ := run.rs.run(run.computer, "pwd", time.Minute); out != run.dir {
			t.Fatalf("the call to computer %s ran in %q; want %s", run.computer, out, run.dir)
		}
		after := developer.readSessionComputer(ctx, run.session, run.computer)
		if after.ExecutionState != "ready" || *after.BindingID != *kept[run.computer].BindingID {
			t.Fatalf("computer %s after the removal %+v; want ready on binding %s", run.computer, after, *kept[run.computer].BindingID)
		}
	}
	var session sessionObject
	developer.get(ctx, "/v1/sessions/"+s1.ID+"?beta=true", &session)
	if len(session.Computers) != 2 || session.Computers[1].ID != c1[1] {
		t.Fatalf("S1 lists computers %+v; want its cloud computer and %s", session.Computers, c1[1])
	}
	record.observe("after the removal S1's computer on %s and both of S2's computers stayed ready on their bindings and ran calls in their directories; S1 lists only its other computers", second.name)
}

// offlineRemoval is a computer removed while its instance was offline, with
// a drive change its machine had not uploaded.
type offlineRemoval struct {
	m          *userMachine
	session    sessionObject
	computerID string
	driveID    string
}

// removeWhileOffline writes a drive change on a Session's user computer
// while uploads fail, stops the machine's App Server, and removes the
// computer, which must answer 202 with the computer unknown.
func removeWhileOffline(t *testing.T, e *engine, set *machines) offlineRemoval {
	t.Helper()
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	m := onlineUserMachine(t, e, set, "removed")
	session, computers, rs := driveSession(t, e, "removed", []sessionDrive{readWrite(driveID)}, m)
	allow := refuseUploads(e, driveID)
	_, envelope := rs.run(computers[0], "printf pending > "+inDrive(driveID, "pending.txt"), 2*time.Minute)
	if sync := envelope.DriveSync; sync == nil || len(sync.NotSynced) != 1 || sync.NotSynced[0].Reason != "uploading" {
		t.Fatalf("drive_sync %+v; want pending.txt not synced, uploading", sync)
	}
	m.server.stop()
	allow()
	m.waitOffline(e)
	e.record.observe("pending.txt is not uploaded and the App Server stopped; the instance is offline")
	status, removed := developer.removeComputer(ctx, session.ID, computers[0])
	if status != http.StatusAccepted || removed.ExecutionState != "unknown" {
		t.Fatalf("removing the computer answered %d %+v; want 202 unknown", status, removed)
	}
	e.record.observe("the removal answered 202 with the computer unknown")
	return offlineRemoval{m: m, session: session, computerID: computers[0], driveID: driveID}
}

// reconnect starts the App Server again and checks that the computer closes,
// that the CLI uploads nothing and deletes the Session's data, and that the
// drive has no pending.txt.
func (r offlineRemoval) reconnect(t *testing.T, e *engine) {
	t.Helper()
	ctx := t.Context()
	developer := e.developer()
	restarted := time.Now()
	r.m.server = r.m.serve()
	developer.waitClosed(ctx, r.session.ID, r.computerID, 2*time.Minute)
	for _, put := range e.edge.answered(http.MethodPut, contentRoute(r.driveID)) {
		if put.at.After(restarted) {
			t.Fatalf("the CLI uploaded %s after it reconnected", put.path)
		}
	}
	if r.m.sessionData(r.session.ID) {
		t.Fatalf("%s still holds the Session's data", r.m.name)
	}
	if status, _ := developer.readFile(ctx, r.driveID, "pending.txt"); status != http.StatusNotFound {
		t.Fatalf("reading pending.txt answered %d; want 404", status)
	}
	e.record.observe("after the reconnect the computer closed; the CLI made no upload, deleted the Session's data, and the drive has no pending.txt")
}

// computerRemovedWhileOfflineClosesOnReconnect removes a computer whose
// instance is offline with a drive change not yet uploaded. The removal
// answers 202 unknown, and the computer stays unknown, and adding the
// instance again answers 409 computer_already_attached_error, until the
// machine reconnects; then the CLI deletes the Session's data without
// uploading the change and the computer closes.
func computerRemovedWhileOfflineClosesOnReconnect(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	removal := removeWhileOffline(t, e, set)
	status, body := developer.addComputer(ctx, removal.session.ID, map[string]any{"instance_id": removal.m.instanceID, "working_directory": removal.m.directory("removed")})
	if status != http.StatusConflict || !strings.Contains(body, "computer_already_attached_error") {
		t.Fatalf("adding the instance again answered %d %s; want 409 computer_already_attached_error", status, body)
	}
	if read := developer.readSessionComputer(ctx, removal.session.ID, removal.computerID); read.ExecutionState != "unknown" {
		t.Fatalf("the removed computer reads %s; want unknown until the reconnect", read.ExecutionState)
	}
	record.observe("adding the instance again answered 409 computer_already_attached_error; the computer read unknown")
	removal.reconnect(t, e)
}

// computerStates reads the association and execution state of a computer
// in-process, since no route serves the computers of an archived Session.
func (e *engine) computerStates(computerID string) (association, execution string) {
	e.t.Helper()
	e.ownerTx(func(ctx context.Context, tx *dbconnect.Tx) error {
		return tx.QueryRow(ctx, `SELECT association_state, execution_state FROM computers WHERE workspace_id=$1 AND id=$2`, harnessWorkspace, computerID).Scan(&association, &execution)
	})
	return association, execution
}

// archivedCloudInstancesTurnUnusedOnceClosed archives a Session whose
// cloud computer holds a drive change whose upload the edge holds, and
// another whose cloud instance is offline. The first instance stays not
// unused while its computer is closing and turns unused, with one unused
// event, once the computer is closed; the second is unused when the archive
// returns.
func archivedCloudInstancesTurnUnusedOnceClosed(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	controller, developer := e.controller(), e.developer()

	offline := set.add("offline", "provider-launcher")
	stopped := developer.createCloudSession(ctx, "offline", offline.directory("workspace"))
	server, stoppedInstance := startClaimedMachine(t, e, e.startRuntime().bind(stopped.sessionID), offline, stopped)
	stopMachine(t, controller, server, stoppedInstance)

	driveID := developer.createDrive(ctx)
	closing := set.add("closing", "provider-launcher")
	held := developer.createCloudSession(ctx, "closing", closing.directory("workspace"), readWrite(driveID))
	rs := e.startRuntime().bind(held.sessionID)
	_, heldInstance := startClaimedMachine(t, e, rs, closing, held)
	content := regexp.MustCompile(`^/v1/client/drives/` + driveID + `/content/` + sha256Hex("pending") + `$`)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool { return r.Method == http.MethodPut && content.MatchString(r.URL.Path) },
		before: func(_ http.ResponseWriter, r *http.Request) bool {
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return false
		},
	})
	if _, written := rs.run(held.computerID, "printf pending > "+inDrive(driveID, "f"), 2*time.Minute); !written.DriveSync.lists(driveID, "f", "uploading") {
		t.Fatalf("the writing command's drive_sync %+v; want f uploading", written.DriveSync)
	}
	events := controller.openInstanceStream(ctx)

	developer.archiveSession(ctx, held.sessionID)
	archived := time.Now()
	for {
		_, execution := e.computerStates(held.computerID)
		unused := controller.readInstance(ctx, heldInstance).Unused
		if execution != "closing" {
			t.Fatalf("computer %s is %s after the archive; want closing while the final upload is held", held.computerID, execution)
		}
		if unused {
			t.Fatalf("instance %s reads unused while its computer is closing", heldInstance)
		}
		if time.Since(archived) > 20*time.Second {
			break
		}
		time.Sleep(time.Second)
	}
	record.observe("for 20 seconds after the archive, computer %s read closing and instance %s not unused", held.computerID, heldInstance)

	developer.archiveSession(ctx, stopped.sessionID)
	if object := controller.readInstance(ctx, stoppedInstance); !object.Unused {
		t.Fatalf("offline instance %s read %+v when the archive returned; want unused", stoppedInstance, object)
	}
	record.observe("offline instance %s read unused when its Session's archive returned", stoppedInstance)

	closed := controller.waitInstance(ctx, heldInstance, 2*time.Minute, "unused", func(object instanceObject) bool { return object.Unused })
	if _, execution := e.computerStates(held.computerID); execution != "closed" {
		t.Fatalf("instance %s reads unused %+v while computer %s is %s; want closed", heldInstance, closed, held.computerID, execution)
	}
	unusedAt := time.Since(archived)
	time.Sleep(5 * time.Second)
	count := map[string]int{}
	for drained := false; !drained; {
		select {
		case event := <-events:
			if event.name == "unused" {
				count[event.instance.ID]++
			}
		default:
			drained = true
		}
	}
	if count[heldInstance] != 1 || count[stoppedInstance] != 1 {
		t.Fatalf("the instance stream carried unused events %v; want one for %s and one for %s", count, heldInstance, stoppedInstance)
	}
	record.observe("computer %s closed and instance %s turned unused %s after the archive; the instance stream carried one unused event for each instance", held.computerID, heldInstance, unusedAt.Round(time.Second))
}

// removalStartsNothingNewAndNeverClosesABlockedCleanup removes two
// computers of a Session at once: one on a machine whose prepared report the
// edge withholds, and one whose background command is released, whose drive
// change upload the edge holds and to which a command is dispatched just
// before the removal. Nothing starts after the CLI admits the close: the
// dispatched command ran only if its tool.execute came before the
// session.close, and a command admitted after the removal runs nowhere.
// The released command's terminal report is accepted and ends its
// background task. The computer in preparation closes. The other stays
// closing while its final upload is blocked and while its closed report is
// withheld; when the cleanup pass ends its expired connection it turns
// unknown, and it stays unknown, never closed, while the machine is offline.
func removalStartsNothingNewAndNeverClosesABlockedCleanup(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	driveID := developer.createDrive(ctx)
	m := onlineUserMachine(t, e, set, "machine")
	other := onlineUserMachine(t, e, set, "other")
	session, computers, rs := driveSession(t, e, "race", []sessionDrive{readWrite(driveID)}, m)
	computer, work := computers[0], m.directory("race")
	binding := *developer.readSessionComputer(ctx, session.ID, computer).BindingID

	otherConnection := controllerConnection(t, e, other.instanceID)
	withheld, stopWithholding := withholdReports(e, func(s protocol.SessionStatus) bool {
		return s.Status == "prepared" && s.ConnectionID == otherConnection
	})
	otherWork := other.directory("race")
	if code, _ := other.shell("mkdir", "-p", otherWork); code != 0 {
		t.Fatalf("mkdir %s exited %d", otherWork, code)
	}
	status, answer := developer.addComputer(ctx, session.ID, map[string]any{"instance_id": other.instanceID, "working_directory": otherWork})
	var preparing computerObject
	if status != http.StatusCreated || json.Unmarshal([]byte(answer), &preparing) != nil {
		t.Fatalf("adding the other machine's computer answered %d %s; want 201", status, answer)
	}
	select {
	case <-withheld:
	case <-time.After(time.Minute):
		t.Fatal("no prepared report of the other machine reached the edge within a minute")
	}
	record.observe("computer %s on the other machine prepared; the edge withholds its prepared report", preparing.ID)

	background := rs.execInput(computer, map[string]any{"cmd": `while [ ! -e bg.release ]; do sleep 0.1; done; echo done`, "wait_ms": 1000})
	if settled := rs.await(background.use, time.Minute); settled.Settlement != "report" || settled.Phase != "running" {
		t.Fatalf("the background command settled %+v; want a running report", settled)
	}
	holdUpload(e, driveID, "held")
	uploadOutside(e, m, session.ID, driveID, "printf held > held.txt && printf sent > sent.txt", 1)
	closed, _ := withholdReports(e, func(s protocol.SessionStatus) bool { return s.Status == "closed" && s.BindingID == binding })

	if code, _ := m.shell("touch", work+"/bg.release"); code != 0 {
		t.Fatal("releasing the background command failed")
	}
	dispatched := rs.execInput(computer, map[string]any{"cmd": "touch dispatched.ran"})
	removed := time.Now()
	if status, object := developer.removeComputer(ctx, session.ID, computer); status != http.StatusAccepted {
		t.Fatalf("removing computer %s answered %d %+v; want 202", computer, status, object)
	}
	if status, object := developer.removeComputer(ctx, session.ID, preparing.ID); status != http.StatusAccepted && status != http.StatusNoContent {
		t.Fatalf("removing computer %s answered %d %+v; want 202 or 204", preparing.ID, status, object)
	}
	late, response := rs.admit(computer, protocol.ToolExecCommand, mustJSON(t, map[string]any{"cmd": "touch late.ran", "on_wait_expiry": "detach"}), scenarioPolicy)
	if response.GetAdmitted() != nil {
		t.Fatalf("a call admitted after the removal answered %v; want it refused", response)
	}
	record.observe("released the background command, dispatched %s, removed both computers and had a later call %s refused at admission: %v", dispatched.use, late, response)

	settled := rs.await(dispatched.use, 2*time.Minute)
	var execAt, closeAt int
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		events := strings.Split(e.edge.streamed(m.instanceID), "\n\n")
		execAt = slices.IndexFunc(events, func(s string) bool {
			return strings.Contains(s, `"tool.execute"`) && strings.Contains(s, dispatched.use)
		})
		closeAt = slices.IndexFunc(events, func(s string) bool { return strings.Contains(s, `"session.close"`) && strings.Contains(s, binding) })
		if closeAt >= 0 || time.Now().After(deadline) {
			break
		}
	}
	ran, _ := m.shell("test", "-e", work+"/dispatched.ran")
	if closeAt < 0 || ran == 0 && (execAt < 0 || execAt > closeAt) {
		t.Fatalf("the dispatched command ran %v; its tool.execute is event %d and the session.close event %d; want it run only when sent before the close", ran == 0, execAt, closeAt)
	}
	if code, _ := m.shell("test", "-e", work+"/late.ran"); code == 0 {
		t.Fatal("the call refused after the removal ran")
	}
	record.observe("the dispatched command ran %v with its tool.execute at event %d before the session.close at event %d, and settled %+v", ran == 0, execAt, closeAt, settled)

	developer.waitClosed(ctx, session.ID, preparing.ID, time.Minute)
	stopWithholding()
	record.observe("computer %s, whose prepared report was withheld, closed", preparing.ID)

	envelope := rs.finished(background, 2*time.Minute)
	if envelope.Status == "" {
		t.Fatalf("the background command ended with %+v; want its terminal result", envelope)
	}
	exit := "none"
	if envelope.Result.ExitCode != nil {
		exit = fmt.Sprint(*envelope.Result.ExitCode)
	}
	record.observe("the background command's terminal report was accepted: %s, exit %s", envelope.Status, exit)

	stateOf := func() string {
		return developer.readSessionComputer(ctx, session.ID, computer).ExecutionState
	}
	for until := removed.Add(25 * time.Second); time.Now().Before(until); time.Sleep(time.Second) {
		if state := stateOf(); state != "closing" {
			t.Fatalf("computer %s reads %s %s after the removal, while its final upload is held; want closing", computer, state, time.Since(removed).Round(time.Second))
		}
	}
	select {
	case <-closed:
	case <-time.After(time.Minute):
		t.Fatal("no closed report reached the edge within a minute of the final upload's end")
	}
	if state := stateOf(); state != "closing" {
		t.Fatalf("computer %s reads %s with its closed report withheld; want closing", computer, state)
	}
	record.observe("computer %s read closing while its final upload was held and while its closed report was withheld", computer)
	endConnection(e, m, func() {
		// The cleanup process's scheduled pass ends the expired connection.
		e.runCleanupPhases(nil)
		developer.waitComputer(ctx, session.ID, computer, time.Minute, "unknown", func(c computerObject) bool { return c.ExecutionState == "unknown" })
		for until := time.Now().Add(15 * time.Second); time.Now().Before(until); time.Sleep(time.Second) {
			if state := stateOf(); state != "unknown" {
				t.Fatalf("computer %s reads %s while its machine is offline; want unknown", computer, state)
			}
		}
	})
	record.observe("once the machine was offline and the cleanup pass ended its expired connection, computer %s turned unknown and stayed unknown, never closed, for 15 seconds while the machine was offline", computer)
}
