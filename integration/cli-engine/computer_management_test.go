package joint

import (
	"testing"
	"time"
)

// TestPersonalAndProviderLaunchersComeOnline runs a product app's launch
// (configure, then server run) and a provider's noninteractive launch (the
// same configure, then server run --idle-exit) of two user instances. Both
// come online while the workspace has no Session. Then, before any recovery
// step, the instance read tells the two delivery outcomes apart: the
// provider instance's replacement credential, installed through configure
// and a restart, is its connection's credential, so a delivery whose
// acknowledgement the Controller lost worked; the personal instance's
// further credential, never delivered, is not in use.
func TestPersonalAndProviderLaunchersComeOnline(t *testing.T) {
	in := requireInputs(t, needs{})
	record := newAcceptanceRecord(t, in, platformOf(in))
	record.boundary("both instances are user instances that POST /v1/instances registered; a cloud instance's claim bootstrap is not served yet")
	record.boundary("credential delivery over a live connection (delivery: stream) is not served yet; only a delivery through configure and a restart is observed")
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
