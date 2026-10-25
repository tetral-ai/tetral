package computerhub

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/hubflow"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
)

// ClientRoutes is what the /v1/client/* routes need: the serving database
// client, the minimum CLI version a ready must meet and the pod's streams.
type ClientRoutes struct {
	Database      *dbconnect.Client
	MinCLIVersion string
	Streams       *Streams
}

const (
	routeClientEvents        = "/v1/client/events"
	routeClientReady         = "/v1/client/ready"
	routeClientHeartbeat     = "/v1/client/heartbeat"
	routeClientProtocolError = "/v1/client/protocol-error"
	routeClientCredential    = "/v1/client/credential" //nolint:gosec // G101: a route path, not credential material.
)

type clientHandler struct {
	ClientRoutes
	logger *slog.Logger
}

func (h *clientHandler) register(r chi.Router) {
	routes := []struct {
		method, pattern string
		handler         http.HandlerFunc
	}{
		{http.MethodGet, routeClientEvents, h.events},
		{http.MethodPost, routeClientReady, h.ready},
		{http.MethodPost, routeClientHeartbeat, h.heartbeat},
		{http.MethodPost, routeClientProtocolError, h.protocolError},
		{http.MethodPost, routeClientCredential, h.credential},
	}
	// Another method on a route path answers the client 404 body.
	for _, route := range routes {
		r.Handle(route.pattern, http.HandlerFunc(writeClientNotFound))
	}
	for _, route := range routes {
		r.Method(route.method, route.pattern, route.handler)
	}
}

// events answers GET /v1/client/events. One transaction locks the
// instance's connection that has not ended FOR UPDATE and applies the
// predecessor rules:
//   - none, or its lease passed: it ends that one (cause lease_expired) and
//     creates a new connection held by this pod; connected carries resumed
//     false;
//   - it is live and previous_connection_id names it with resume=true: its
//     lease moves to this pod's stream without renewal and the previous pod
//     closes its stream; connected carries the same connection and resumed
//     true, and the stream writes every unreported instruction again;
//   - it is live and previous_connection_id names it with resume=false:
//     409 previous_connection_active with retry_after_ms the remaining lease
//     in milliseconds, rounded up;
//   - it is live and no previous_connection_id, or another one, was given:
//     409 connection_conflict.
//
// A refusal changes nothing. A request authenticated with a pending
// credential, a renewal whose key was fetched, activates it in the
// transaction that moves the lease or creates the connection: the credential
// becomes active and the connection's, and the credential it replaces is
// revoked unless it has expired (instance.ActivateCredential); a refused
// request activates
// nothing. A draining pod answers 503 before the transaction; a stream
// registered after its commit while the pod began to drain writes connected
// and then its stream-only disconnect, so the CLI resumes elsewhere.
func (h *clientHandler) events(w http.ResponseWriter, r *http.Request) {
	ctx, id := r.Context(), identityOf(r)
	query, err := protocol.DecodeEventsQuery(r.URL.Query())
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	if query.InstanceID != id.instanceID || query.CredentialID != id.credentialID {
		writeClientFailure(w, r, refuse(protocol.ErrorCredentialInvalid, "the connection key does not authenticate the named instance and credential"))
		return
	}
	if !h.Streams.accepting() {
		httpapi.WriteError(w, r, &httpapi.UnavailableError{Message: "the pod is shutting down"})
		return
	}
	var (
		started   instance.Connection
		resumed   bool
		activated string
		replaced  string
	)
	err = h.clientTx(ctx, id.workspace, "computer_hub.client_events", func(tx *dbconnect.Tx) error {
		activated, replaced = "", ""
		granted, err := authorize(ctx, tx, id, true, false)
		if err != nil {
			return err
		}
		activating := granted.credential.State == instance.CredentialPending
		if activating && granted.credential.ReplacesCredentialID != nil {
			// Locked before the connection, the order every request keeps.
			if _, err := instance.LockCredential(ctx, tx, id.workspace, id.instanceID, *granted.credential.ReplacesCredentialID); err != nil {
				return err
			}
		}
		activate := func() error {
			if !activating {
				return nil
			}
			activated = granted.credential.ID
			replaced, err = instance.ActivateCredential(ctx, tx, id.workspace, granted.credential, granted.now)
			return err
		}
		previous, found, err := instance.LockUnendedConnection(ctx, tx, id.workspace, id.instanceID)
		if err != nil {
			return err
		}
		if found && previous.Live(granted.now) {
			if !query.PreviousConnectionID.Set || query.PreviousConnectionID.Value != previous.ID {
				return refuse(protocol.ErrorConnectionConflict, "another connection of the instance is active")
			}
			if query.Resume.Value != "true" {
				remaining := (previous.LeaseExpiresAt.Sub(granted.now) + time.Millisecond - 1) / time.Millisecond
				return refuseRetryAfter(protocol.ErrorPreviousConnectionActive, "the previous connection's lease is still active", int64(remaining))
			}
			if err := activate(); err != nil {
				return err
			}
			credentialID := previous.CredentialID
			if activating {
				credentialID = granted.credential.ID
			}
			started, err = instance.ResumeConnection(ctx, tx, id.workspace, previous, h.Streams.podName, credentialID)
			resumed = true
			return err
		}
		if found {
			if err := hubflow.EndConnection(ctx, tx, id.workspace, previous, instance.EndLeaseExpired); err != nil {
				return err
			}
		}
		leaseExpiresAt, _, _ := instance.Grant(granted.now, granted.credential.ExpiresAt)
		if !leaseExpiresAt.After(granted.now) {
			return refuse(protocol.ErrorCredentialExpired, "no lease grant remains before the credential expires")
		}
		if err := activate(); err != nil {
			return err
		}
		started, err = instance.CreateConnection(ctx, tx, id.workspace, instance.NewConnection{
			ID: instance.NewConnectionID(), InstanceID: id.instanceID, CredentialID: id.credentialID, HubPod: h.Streams.podName,
			ConnectedAt: granted.now, LeaseExpiresAt: leaseExpiresAt,
		})
		return err
	})
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	h.auditActivation(ctx, id, activated, replaced)
	stream := h.Streams.hold(id.workspace, started.ID, started.StreamCount)
	if stream == nil {
		httpapi.WriteError(w, r, &httpapi.UnavailableError{Message: "the pod is shutting down"})
		return
	}
	defer h.Streams.release(stream)
	h.Streams.serve(ctx, w, stream, started, resumed)
}
