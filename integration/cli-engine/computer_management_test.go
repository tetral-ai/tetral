package joint

import (
	"context"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// TestPersonalAndProviderLaunchersComeOnline runs a product app's launch
// (configure, then server run) and a provider's noninteractive launch (the
// same configure, then server run --idle-exit) of two user instances. Both
// come online while the workspace has no Session. Then, before any recovery
// step, the instance read tells the two delivery outcomes apart: the
// provider instance's replacement credential, installed through configure
// and a restart, is its connection's credential, so a delivery whose
// acknowledgement the Controller lost worked; the personal instance's
// further credential, never delivered, is not in use. Last, the provider's
// launch configures the bootstrap of a claim and its cloud instance comes
// online holding no Session.
func TestPersonalAndProviderLaunchersComeOnline(t *testing.T) {
	in := requireInputs(t, needs{})
	record := newAcceptanceRecord(t, in, platformOf(in))
	record.boundary("the claimed request is recorded in-process with computer.EnsureRequest, in place of tool-call admission")
	record.boundary("this scenario observes only credential delivery through configure and a restart; it does not exercise delivery over a live connection (delivery: stream)")
	record.boundary("the idle exit itself does not happen: the run ends before 15 minutes without a command")
	e := startEngine(t, record, engineOptions{})
	controller, developer := e.controller(), e.developer()
	set := newMachines(t, in, needs{}, e, record)
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
	record.observe("the provider launcher installed replacement credential %s with configure and restarted; online again after %s, its first connection waiting out the previous connection's lease", delivered.ID, time.Since(restarted).Round(time.Second))
	requireOnlyInUse(t, controller, replaced.instance.ID, delivered.ID, replaced.credential.ID)
	record.observe("provider instance read: connection.credential_id is the delivered %s, which alone is in_use; the first %s is not in use; none is revoked", delivered.ID, replaced.credential.ID)

	personal := registrations["personal"]
	undelivered := controller.issueResponseCredential(ctx, personal.instance.ID)
	requireOnlyInUse(t, controller, personal.instance.ID, personal.credential.ID, undelivered.ID)
	record.observe("personal instance read: connection.credential_id is still %s, which alone is in_use; the undelivered %s is not in use; none is revoked", personal.credential.ID, undelivered.ID)

	s := developer.createCloudSession(ctx, "provider-claim")
	claimed := controller.claimRequest(ctx, e.recordRequest(s).ID, "provider-worker")
	cloudInstance, cloudCredential := claimed.bootstrapFields(t)
	cloud := set.add("provider-claim", "provider-launcher")
	cloud.configure(claimed.Bootstrap)
	cloud.serve("--idle-exit", "15m")
	requireCloudOnline(t, controller, cloud, cloudInstance, cloudCredential, s.computerID)
	record.observe("the provider launcher configured the bootstrap of a claim for cloud instance %s and came online with binding_count 0: it holds no Session", cloudInstance)
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

// TestRevocationEndsTheLiveConnectionAtOnce revokes a CLI's credential just
// before its next heartbeat is due. The instance read shows the connection
// offline as soon as the revocation returns; no heartbeat is accepted and no
// lease granted after the revocation; the CLI blocks with
// credential_revoked and opens no other connection.
func TestRevocationEndsTheLiveConnectionAtOnce(t *testing.T) {
	in := requireInputs(t, needs{})
	record := newAcceptanceRecord(t, in, platformOf(in))
	record.boundary("no dispatch races the revocation: Engine does not route tool calls to a computer yet")
	e := startEngine(t, record, engineOptions{})
	controller := e.controller()
	set := newMachines(t, in, needs{}, e, record)
	ctx := t.Context()

	registered := controller.createUserInstance(ctx, "revoked")
	m := set.add("revoked", "revoked-app")
	m.configure(bootstrap(e.serverURL, registered.instance.ID, registered.credential))
	m.serve()
	requireOnline(t, controller, m, registered.instance.ID, registered.credential.ID)
	beat := controller.waitInstance(ctx, registered.instance.ID, 45*time.Second, "a first accepted heartbeat", func(object instanceObject) bool {
		return object.Connection != nil && object.Connection.LastHeartbeatAt != nil
	})
	connection := beat.Connection
	due := connection.LastHeartbeatAt.Add(15 * time.Second)
	time.Sleep(time.Until(due.Add(-20 * time.Millisecond)))
	controller.revokeCredential(ctx, registered.instance.ID, registered.credential.ID)

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

// requireOnline waits until the machine's status and the instance read both
// show the connection online with credentialID, and checks what the read
// reports of it.
func requireOnline(t *testing.T, controller *controllerClient, m *machine, instanceID, credentialID string) {
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
	case object.ComputerCount != 0 || object.BindingCount != 0:
		t.Fatalf("instance read computer_count %d binding_count %d; want 0 and 0", object.ComputerCount, object.BindingCount)
	case object.Connection.ReadyAt == nil:
		t.Fatalf("instance read connection %+v has no ready_at", object.Connection)
	}
	m.record.observe("%s: status online with connection %s; instance read online with credential %s, cli_version %s, ready_at %s, no computer and no binding", m.name, object.Connection.ID, credentialID, *object.CLIVersion, object.Connection.ReadyAt.Format(time.RFC3339Nano))
}

// recordRequest records the open request of the Session's cloud computer
// in-process as the table owner, as tool-call admission does.
func (e *engine) recordRequest(s cloudSession) computer.Request {
	e.t.Helper()
	var request computer.Request
	e.ownerTx(func(ctx context.Context, tx *dbconnect.Tx) error {
		var err error
		request, err = computer.EnsureRequest(ctx, tx, workspace.ID(harnessWorkspace), s.sessionID, s.computerID)
		return err
	})
	e.record.command("in-process `computer.EnsureRequest` for computer %s, in place of tool-call admission: %s request %s", s.computerID, request.Kind, request.ID)
	return request
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

// ownerTx runs fn in a transaction of the harness workspace as the table
// owner.
func (e *engine) ownerTx(fn func(context.Context, *dbconnect.Tx) error) {
	e.t.Helper()
	ctx := e.t.Context()
	if e.owner == nil {
		opened, err := dbconnect.OpenProtectedDSN(ctx, e.ownerURL, e.databaseCA, postgresName)
		if err != nil {
			e.t.Fatalf("open the database as the table owner: %v", err)
		}
		e.owner = opened.Client
		e.t.Cleanup(func() { _ = e.owner.Close() })
	}
	if err := e.owner.WithWorkspaceTx(ctx, harnessWorkspace, "joint.owner_read_write", func(tx *dbconnect.Tx) error { return fn(ctx, tx) }); err != nil {
		e.t.Fatalf("owner transaction: %v", err)
	}
}

// requireCloudOnline waits until the machine's status and the instance read
// both show the connection online with credentialID, and checks that the
// instance is the active cloud instance of computerID, which holds it as
// current, with no binding.
func requireCloudOnline(t *testing.T, controller *controllerClient, m *machine, instanceID, credentialID, computerID string) instanceObject {
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
	case object.Connection.ID != statusField(status, "connection_id") || object.Connection.CredentialID != credentialID:
		t.Fatalf("instance read connection %+v; the CLI's status names connection %s with credential %s", object.Connection, status["connection_id"], credentialID)
	case object.Kind != "cloud" || object.ComputerID == nil || *object.ComputerID != computerID || object.RegistrationState != "active":
		t.Fatalf("instance read kind %s computer_id %v registration %s; want an active cloud instance of %s", object.Kind, object.ComputerID, object.RegistrationState, computerID)
	case object.ComputerCount != 1 || object.BindingCount != 0 || object.Unused:
		t.Fatalf("instance read computer_count %d binding_count %d unused %v; want 1, 0 and false", object.ComputerCount, object.BindingCount, object.Unused)
	}
	m.record.observe("%s: status online with connection %s; instance read online with credential %s, kind cloud, computer_id %s, computer_count 1, binding_count 0, not unused", m.name, object.Connection.ID, credentialID, computerID)
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

// startClaimedMachine claims a new request of the Session's computer and
// starts a provider launch with the claim's bootstrap; it returns the
// running server and the claim's instance once online.
func startClaimedMachine(t *testing.T, e *engine, set *machines, s cloudSession, name string) (*cliProcess, string) {
	t.Helper()
	controller := e.controller()
	request := e.recordRequest(s)
	claimed := controller.claimRequest(t.Context(), request.ID, "worker-"+name)
	instanceID, credentialID := claimed.bootstrapFields(t)
	m := set.add(name, "provider-launcher")
	m.configure(claimed.Bootstrap)
	server := m.serve("--idle-exit", "15m")
	requireCloudOnline(t, controller, m, instanceID, credentialID, s.computerID)
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

// TestControllerStartsAMachineForANewRequest records a new request for a
// Session's cloud computer. A Controller reads it from the Environment's
// stream and list, claims it and starts a machine with the claim's
// bootstrap. The machine comes online as the cloud instance of that
// computer, the request is fulfilled and the instance is the computer's
// current instance.
func TestControllerStartsAMachineForANewRequest(t *testing.T) {
	in := requireInputs(t, needs{})
	record := newAcceptanceRecord(t, in, platformOf(in))
	record.boundary("the request is recorded in-process with computer.EnsureRequest; no machine-bound call waits for it, since Engine does not route tool calls to a computer yet")
	e := startEngine(t, record, engineOptions{})
	controller, developer := e.controller(), e.developer()
	set := newMachines(t, in, needs{}, e, record)
	ctx := t.Context()

	s := developer.createCloudSession(ctx, "new-request")
	events := controller.openRequestStream(ctx, s.environmentID)
	request := e.recordRequest(s)
	if created := nextRequestEvent(t, events); created.name != "created" || created.request.ID != request.ID || created.request.Kind != "new" {
		t.Fatalf("request stream event %+v; want created of the new request %s", created, request.ID)
	}
	if listed := controller.listRequests(ctx, s.environmentID); len(listed) != 1 || listed[0].ID != request.ID || listed[0].State != "pending" {
		t.Fatalf("pending requests of %s: %+v; want only %s", s.environmentID, listed, request.ID)
	}
	record.observe("the stream of %s delivered created for new request %s, which the list shows as the one pending request", s.environmentID, request.ID)

	claimed := controller.claimRequest(ctx, request.ID, "worker-1")
	instanceID, credentialID := claimed.bootstrapFields(t)
	if claimed.Request.State != "claimed" || claimed.Request.InstanceID == nil || *claimed.Request.InstanceID != instanceID || claimed.WorkingDirectory == nil {
		t.Fatalf("claim result %+v; want the claimed request of instance %s with a working directory", claimed, instanceID)
	}
	if event := nextRequestEvent(t, events); event.name != "claimed" || event.request.ID != request.ID {
		t.Fatalf("request stream event %+v; want claimed of %s", event, request.ID)
	}
	m := set.add("cloud", "provider-launcher")
	m.configure(claimed.Bootstrap)
	m.serve("--idle-exit", "15m")
	requireCloudOnline(t, controller, m, instanceID, credentialID, s.computerID)

	fulfilled := e.readRequest(request.ID)
	current := developer.readComputer(ctx, s).CurrentInstanceID
	if fulfilled.State != computer.RequestFulfilled || current == nil || *current != instanceID {
		t.Fatalf("request %s %s, computer current_instance_id %v; want fulfilled and %s", request.ID, fulfilled.State, current, instanceID)
	}
	record.observe("request %s fulfilled by the ready of %s, which the computer %s reads as current_instance_id; working_directory %s", request.ID, instanceID, s.computerID, *claimed.WorkingDirectory)
}

// TestStoppedCloudInstanceWakesWithItsIdentity stops a cloud instance's
// machine and records the computer's next request: a wake request for that
// instance. A Controller claims it and restarts the machine with the claim's
// bootstrap; the machine comes online as the same instance under the claim's
// credential, whose first use revokes the instance's earlier credential, and
// the request is fulfilled.
func TestStoppedCloudInstanceWakesWithItsIdentity(t *testing.T) {
	in := requireInputs(t, needs{})
	record := newAcceptanceRecord(t, in, platformOf(in))
	record.boundary("the requests are recorded in-process with computer.EnsureRequest; no machine-bound call waits for them, since Engine does not route tool calls to a computer yet")
	record.boundary("the machine is stopped with SIGTERM, not by its idle exit")
	e := startEngine(t, record, engineOptions{})
	controller, developer := e.controller(), e.developer()
	set := newMachines(t, in, needs{}, e, record)
	ctx := t.Context()

	s := developer.createCloudSession(ctx, "wake-request")
	server, instanceID := startClaimedMachine(t, e, set, s, "cloud")
	first := controller.readInstance(ctx, instanceID).Connection.CredentialID
	stopMachine(t, controller, server, instanceID)
	record.observe("the machine of cloud instance %s stopped; the instance reads offline", instanceID)

	wake := e.recordRequest(s)
	if wake.Kind != computer.RequestWake || wake.InstanceID == nil || *wake.InstanceID != instanceID {
		t.Fatalf("request after the stop %+v; want a wake request for %s", wake, instanceID)
	}
	claimed := controller.claimRequest(ctx, wake.ID, "worker-wake")
	wakeInstance, wakeCredential := claimed.bootstrapFields(t)
	if wakeInstance != instanceID || claimed.WorkingDirectory != nil {
		t.Fatalf("wake claim result %+v; want a bootstrap for %s without a working directory", claimed, instanceID)
	}
	m := server.machine
	m.configure(claimed.Bootstrap)
	m.serve("--idle-exit", "15m")
	requireCloudOnline(t, controller, m, instanceID, wakeCredential, s.computerID)

	revoked := false
	for _, credential := range controller.listCredentials(ctx, instanceID) {
		revoked = revoked || credential.ID == first && credential.RevokedAt != nil
	}
	if !revoked || e.readRequest(wake.ID).State != computer.RequestFulfilled {
		t.Fatalf("after the restart: earlier credential %s revoked %v, wake request %s; want revoked and fulfilled", first, revoked, e.readRequest(wake.ID).State)
	}
	record.observe("wake request %s claimed and fulfilled: the machine restarted with the claim's credential %s as the same instance %s, and that credential's first use revoked the earlier %s", wake.ID, wakeCredential, instanceID, first)
}

// TestFailedWakeRequestLeavesTheInstanceOffline stops a cloud instance's
// machine; a Controller claims the computer's wake request and fails it.
// The claim's credential is revoked, the computer keeps the instance as
// current, the instance stays offline and not unused, the Session gets no
// machine message, no session.computer_replaced and no session.error, and
// the computer's next request is a new wake request for the same instance.
func TestFailedWakeRequestLeavesTheInstanceOffline(t *testing.T) {
	in := requireInputs(t, needs{})
	record := newAcceptanceRecord(t, in, platformOf(in))
	record.boundary("the requests are recorded in-process with computer.EnsureRequest; no machine-bound call waits for them, since Engine does not route tool calls to a computer yet")
	record.boundary("only a failed wake request is observed; a wake request's expiry belongs to the cleanup phase, which does not run yet")
	e := startEngine(t, record, engineOptions{})
	controller, developer := e.controller(), e.developer()
	set := newMachines(t, in, needs{}, e, record)
	ctx := t.Context()

	s := developer.createCloudSession(ctx, "failed-wake")
	server, instanceID := startClaimedMachine(t, e, set, s, "cloud")
	stopMachine(t, controller, server, instanceID)
	wake := e.recordRequest(s)
	claimed := controller.claimRequest(ctx, wake.ID, "worker-wake")
	_, claimCredential := claimed.bootstrapFields(t)
	if failed := controller.failRequest(ctx, wake.ID); failed.State != "failed" {
		t.Fatalf("fail answered %+v; want failed", failed)
	}

	revoked := false
	for _, credential := range controller.listCredentials(ctx, instanceID) {
		revoked = revoked || credential.ID == claimCredential && credential.RevokedAt != nil
	}
	object := controller.readInstance(ctx, instanceID)
	current := developer.readComputer(ctx, s).CurrentInstanceID
	switch {
	case !revoked:
		t.Fatalf("the failed claim's credential %s is not revoked", claimCredential)
	case current == nil || *current != instanceID:
		t.Fatalf("computer current_instance_id %v after the failed wake; want %s", current, instanceID)
	case object.Connection == nil || object.Connection.State != "offline" || object.Unused:
		t.Fatalf("instance %s after the failed wake: connection %+v, unused %v; want offline and not unused", instanceID, object.Connection, object.Unused)
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
	record.observe("wake request %s failed: claim credential %s revoked; computer %s keeps current_instance_id %s, offline and not unused; no machine message, session.computer_replaced or session.error in the Session's tables", wake.ID, claimCredential, s.computerID, instanceID)

	next := e.recordRequest(s)
	if next.ID == wake.ID || next.Kind != computer.RequestWake || next.InstanceID == nil || *next.InstanceID != instanceID {
		t.Fatalf("next request %+v; want a new wake request for %s", next, instanceID)
	}
	record.observe("the computer's next request %s is a new wake request for %s", next.ID, instanceID)
}
