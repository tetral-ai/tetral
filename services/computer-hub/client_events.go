package computerhub

import (
	"log/slog"
	"net/http"

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
	}
	// Another method on a route path answers the client 404 body.
	for _, route := range routes {
		r.Handle(route.pattern, http.HandlerFunc(writeClientNotFound))
	}
	for _, route := range routes {
		r.Method(route.method, route.pattern, route.handler)
	}
}

// events answers GET /v1/client/events. When the instance has no live
// connection, one transaction ends a connection whose lease passed (cause
// lease_expired) and creates a new one held by this pod, whose stream then
// starts with connected (resumed false). A live connection answers 409
// connection_conflict and is not replaced.
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
	connectionID := instance.NewConnectionID()
	stream := h.Streams.hold(id.workspace, connectionID)
	if stream == nil {
		httpapi.WriteError(w, r, &httpapi.UnavailableError{Message: "the pod is shutting down"})
		return
	}
	defer h.Streams.release(stream)
	var started instance.Connection
	err = h.Database.WithWorkspaceTx(ctx, string(id.workspace), "computer_hub.client_events", func(tx *dbconnect.Tx) error {
		granted, err := authorize(ctx, tx, id)
		if err != nil {
			return err
		}
		previous, found, err := instance.LockUnendedConnection(ctx, tx, id.workspace, id.instanceID)
		if err != nil {
			return err
		}
		if found {
			if previous.Live(granted.now) {
				return refuse(protocol.ErrorConnectionConflict, "another connection of the instance is active")
			}
			if err := hubflow.EndConnection(ctx, tx, id.workspace, previous, instance.EndLeaseExpired); err != nil {
				return err
			}
		}
		leaseExpiresAt, _, _ := instance.Grant(granted.now, granted.credential.ExpiresAt)
		if !leaseExpiresAt.After(granted.now) {
			return refuse(protocol.ErrorCredentialExpired, "no lease grant remains before the credential expires")
		}
		started, err = instance.CreateConnection(ctx, tx, id.workspace, instance.NewConnection{
			ID: connectionID, InstanceID: id.instanceID, CredentialID: id.credentialID, HubPod: h.Streams.podName,
			ConnectedAt: granted.now, LeaseExpiresAt: leaseExpiresAt,
		})
		return err
	})
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	h.Streams.serve(ctx, w, stream, started, false)
}
