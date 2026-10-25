package joint

import (
	"net/http"
	"testing"
	"time"
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
	record.boundary("no task runs and no instruction waits when the pod dies: Engine does not prepare Sessions or route tool calls to a computer yet")
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
	record.boundary("no command runs across the disconnect: Engine does not route tool calls to a computer yet")
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

// TestDelayedReadyAcknowledgementKeepsTheConnection holds every answer to
// POST /v1/client/ready at the edge for 8 seconds, inside the CLI's
// 10-second request deadline. The CLI reaches online on its first connection
// once the acknowledgement arrives and opens no other connection.
func TestDelayedReadyAcknowledgementKeepsTheConnection(t *testing.T) {
	in := requireInputs(t, needs{})
	record := newAcceptanceRecord(t, in, platformOf(in))
	record.boundary("no Session has a computer on the instance: Engine does not prepare Sessions yet, so neither the 64-instruction limit before the first Session report nor 70 computers becoming ready is observed")
	e := startEngine(t, record, engineOptions{})
	e.edge.delay("/v1/client/ready", 8*time.Second)
	controller := e.controller()
	set := newMachines(t, in, needs{}, e, record)
	ctx := t.Context()

	registered := controller.createUserInstance(ctx, "delayed")
	m := set.add("delayed", "delayed-app")
	m.configure(bootstrap(e.serverURL, registered.instance.ID, registered.credential))
	m.serve()
	requireOnline(t, controller, m, registered.instance.ID, registered.credential.ID)
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
