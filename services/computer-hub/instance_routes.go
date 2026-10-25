package computerhub

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// InstanceRoutes is what the instance routes need: the serving database
// client, the settings they apply and the pod's instance event fan-out.
type InstanceRoutes struct {
	Database              *dbconnect.Client
	MaxCredentialLifetime time.Duration
	PageTokenKey          []byte
	// RecommendedCLIVersion decides each instance read's
	// cli_update_available.
	RecommendedCLIVersion string
	Events                *InstanceEvents
}

const (
	routeInstances           = "/v1/instances"
	routeInstance            = "/v1/instances/{instance_id}"
	routeInstanceStream      = "/v1/instances/stream"
	routeInstanceCredentials = "/v1/instances/{instance_id}/credentials"
	routeInstanceCredential  = "/v1/instances/{instance_id}/credentials/{credential_id}"
)

const maxInstanceNameLength = 256

type instanceHandler struct {
	InstanceRoutes
	logger *slog.Logger
}

func (h *instanceHandler) register(r chi.Router) {
	routes := []struct {
		method, pattern string
		handler         http.HandlerFunc
	}{
		{http.MethodPost, routeInstances, h.createInstance},
		{http.MethodGet, routeInstances, h.listInstances},
		{http.MethodGet, routeInstanceStream, h.streamInstances},
		{http.MethodGet, routeInstance, h.readInstance},
		{http.MethodDelete, routeInstance, h.deregisterInstance},
		{http.MethodPost, routeInstanceCredentials, h.issueCredential},
		{http.MethodGet, routeInstanceCredentials, h.listCredentials},
		{http.MethodDelete, routeInstanceCredential, h.revokeCredential},
	}
	// Another method on a route path answers 404 through this group, so the
	// credential class check still runs first.
	for _, route := range routes {
		r.Handle(route.pattern, http.HandlerFunc(writeManagementNotFound))
	}
	for _, route := range routes {
		r.Method(route.method, route.pattern, httpapi.DeclarePublicOperation(route.method, route.pattern, route.handler))
	}
}

func (h *instanceHandler) createInstance(w http.ResponseWriter, r *http.Request) {
	ctx, principal := r.Context(), mustPrincipal(r)
	ws := principal.Workspace.ID
	body, err := httpapi.ReadJSONBody(w, r)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	key, err := idempotencyKey(r)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	scope := instance.IdempotencyScope{PrincipalCredentialID: principal.Credential.ID, Route: r.Method + " " + r.URL.Path, Key: key}
	fingerprint, canonical := instance.Fingerprint(body)
	var issued issuance
	err = h.idempotentTx(ctx, ws, "computer_hub.instances_create", func(tx *dbconnect.Tx) error {
		issued = issuance{}
		if replayed, found, err := h.replay(ctx, tx, ws, scope, fingerprint, canonical); found || err != nil {
			issued = replayed
			return err
		}
		if !canonical {
			return invalidBody()
		}
		fields, err := decodeObject(body, "name", "credential")
		if err != nil {
			return err
		}
		name, err := instanceName(fields["name"])
		if err != nil {
			return err
		}
		credentialRaw, ok := fields["credential"]
		if !ok {
			return &httpapi.ValidationError{Message: "credential must be an object"}
		}
		credentialFields, err := decodeObjectField(credentialRaw, "credential", "expires_at")
		if err != nil {
			return err
		}
		requested, err := requestedExpiry(credentialFields)
		if err != nil {
			return err
		}
		now, err := instance.Now(ctx, tx)
		if err != nil {
			return err
		}
		expiresAt, ok := instance.ResolveExpiry(now, requested, h.MaxCredentialLifetime)
		if !ok {
			return h.expiryError()
		}
		created, err := instance.CreateUserInstance(ctx, tx, ws, name)
		if err != nil {
			return err
		}
		credential, connectionKey, err := instance.IssueCredential(ctx, tx, ws, created.ID, instance.CredentialActive, now, expiresAt)
		if err != nil {
			return err
		}
		issued = issuance{instance: created, credential: credential, key: &connectionKey, now: now}
		if issued.object, err = instance.ReadObject(ctx, tx, ws, created, h.RecommendedCLIVersion, now); err != nil {
			return err
		}
		return instance.SaveIdempotencyRecord(ctx, tx, ws, scope, instance.IdempotencyRecord{Fingerprint: fingerprint, InstanceID: created.ID, CredentialID: credential.ID, Status: http.StatusCreated})
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	h.auditIssue(ctx, principal, issued)
	writeJSON(w, http.StatusCreated, struct {
		Instance   instance.InstanceObject         `json:"instance"`
		Credential instance.IssuedCredentialObject `json:"credential"`
	}{issued.object, issued.credentialObject()})
}

func (h *instanceHandler) issueCredential(w http.ResponseWriter, r *http.Request) {
	ctx, principal := r.Context(), mustPrincipal(r)
	ws, instanceID := principal.Workspace.ID, chi.URLParam(r, "instance_id")
	body, err := httpapi.ReadJSONBody(w, r)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	scope := instance.IdempotencyScope{PrincipalCredentialID: principal.Credential.ID, Route: r.Method + " " + r.URL.Path}
	fingerprint, canonical := instance.Fingerprint(body)
	var issued issuance
	err = h.idempotentTx(ctx, ws, "computer_hub.instance_credentials_create", func(tx *dbconnect.Tx) error {
		issued = issuance{}
		current, err := instance.LockInstance(ctx, tx, ws, instanceID)
		if err != nil {
			return err
		}
		if scope.Key, err = idempotencyKey(r); err != nil {
			return err
		}
		if replayed, found, err := h.replay(ctx, tx, ws, scope, fingerprint, canonical); found || err != nil {
			issued = replayed
			return err
		}
		if current.RegistrationState == instance.RegistrationDeregistered {
			return &instance.UnavailableError{}
		}
		if !canonical {
			return invalidBody()
		}
		fields, err := decodeObject(body, "delivery", "expires_at")
		if err != nil {
			return err
		}
		delivery, err := credentialDelivery(fields)
		if err != nil {
			return err
		}
		requested, err := requestedExpiry(fields)
		if err != nil {
			return err
		}
		now, err := instance.Now(ctx, tx)
		if err != nil {
			return err
		}
		expiresAt, ok := instance.ResolveExpiry(now, requested, h.MaxCredentialLifetime)
		if !ok {
			return h.expiryError()
		}
		if delivery == instance.DeliveryStream {
			if err := instance.CheckCredentialLimit(ctx, tx, ws, instanceID, now); err != nil {
				return err
			}
			// Stream delivery needs an online connection, and no connection
			// is online until the hub serves CLI connections.
			return &instance.OfflineError{}
		}
		credential, connectionKey, err := instance.IssueCredential(ctx, tx, ws, instanceID, instance.CredentialActive, now, expiresAt)
		if err != nil {
			return err
		}
		issued = issuance{instance: current, credential: credential, key: &connectionKey, now: now}
		return instance.SaveIdempotencyRecord(ctx, tx, ws, scope, instance.IdempotencyRecord{Fingerprint: fingerprint, InstanceID: instanceID, CredentialID: credential.ID, Status: http.StatusCreated})
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	h.auditIssue(ctx, principal, issued)
	writeJSON(w, http.StatusCreated, issued.credentialObject())
}

func (h *instanceHandler) listInstances(w http.ResponseWriter, r *http.Request) {
	ctx, ws := r.Context(), mustPrincipal(r).Workspace.ID
	binding := pageBinding{WorkspaceID: string(ws), Route: http.MethodGet + " " + routeInstances, Filters: map[string]string{}, Order: pageOrder}
	page, err := parseListPage(r, h.PageTokenKey, binding)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	response := listResponse[instance.InstanceObject]{Data: []instance.InstanceObject{}}
	err = h.Database.WithWorkspaceReadOnlyTx(ctx, string(ws), "computer_hub.instances_list", func(tx *dbconnect.Tx) error {
		rows, err := instance.ListInstances(ctx, tx, ws, page.after, page.limit+1)
		if err != nil {
			return err
		}
		now, err := instance.Now(ctx, tx)
		if err != nil {
			return err
		}
		more := len(rows) > page.limit
		rows = rows[:min(len(rows), page.limit)]
		objects, err := instance.ReadObjects(ctx, tx, ws, rows, h.RecommendedCLIVersion, now)
		if err != nil {
			return err
		}
		response.Data = append(response.Data, objects...)
		if more {
			response.NextPage = nextPage(h.PageTokenKey, binding, rows[len(rows)-1].StorageSequence, true)
		}
		return nil
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *instanceHandler) readInstance(w http.ResponseWriter, r *http.Request) {
	ctx, ws := r.Context(), mustPrincipal(r).Workspace.ID
	var object instance.InstanceObject
	err := h.Database.WithWorkspaceReadOnlyTx(ctx, string(ws), "computer_hub.instances_read", func(tx *dbconnect.Tx) error {
		row, err := instance.ReadInstance(ctx, tx, ws, chi.URLParam(r, "instance_id"))
		if err != nil {
			return err
		}
		now, err := instance.Now(ctx, tx)
		if err != nil {
			return err
		}
		object, err = instance.ReadObject(ctx, tx, ws, row, h.RecommendedCLIVersion, now)
		return err
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, object)
}

func (h *instanceHandler) deregisterInstance(w http.ResponseWriter, r *http.Request) {
	ctx, principal := r.Context(), mustPrincipal(r)
	ws, instanceID := principal.Workspace.ID, chi.URLParam(r, "instance_id")
	var revoked []string
	err := h.Database.WithWorkspaceTx(ctx, string(ws), "computer_hub.instances_delete", func(tx *dbconnect.Tx) error {
		var err error
		revoked, err = instance.Deregister(ctx, tx, ws, instanceID)
		return err
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	for _, credentialID := range revoked {
		instance.AuditCredentialOperation(ctx, h.logger, actorOf(principal), instanceID, credentialID, instance.AuditRevoke)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *instanceHandler) listCredentials(w http.ResponseWriter, r *http.Request) {
	ctx, ws := r.Context(), mustPrincipal(r).Workspace.ID
	instanceID := chi.URLParam(r, "instance_id")
	binding := pageBinding{WorkspaceID: string(ws), Route: http.MethodGet + " " + routeInstanceCredentials, Filters: map[string]string{"instance_id": instanceID}, Order: pageOrder}
	page, err := parseListPage(r, h.PageTokenKey, binding)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	response := listResponse[instance.CredentialObject]{Data: []instance.CredentialObject{}}
	err = h.Database.WithWorkspaceReadOnlyTx(ctx, string(ws), "computer_hub.instance_credentials_list", func(tx *dbconnect.Tx) error {
		if _, err := instance.ReadInstance(ctx, tx, ws, instanceID); err != nil {
			return err
		}
		rows, err := instance.ListCredentials(ctx, tx, ws, instanceID, page.after, page.limit+1)
		if err != nil {
			return err
		}
		now, err := instance.Now(ctx, tx)
		if err != nil {
			return err
		}
		live, err := instance.LiveCredentialID(ctx, tx, ws, instanceID, now)
		if err != nil {
			return err
		}
		more := len(rows) > page.limit
		rows = rows[:min(len(rows), page.limit)]
		for _, row := range rows {
			response.Data = append(response.Data, instance.CredentialObjectOf(row, live, now))
		}
		if more {
			response.NextPage = nextPage(h.PageTokenKey, binding, rows[len(rows)-1].StorageSequence, true)
		}
		return nil
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *instanceHandler) revokeCredential(w http.ResponseWriter, r *http.Request) {
	ctx, principal := r.Context(), mustPrincipal(r)
	ws, instanceID, credentialID := principal.Workspace.ID, chi.URLParam(r, "instance_id"), chi.URLParam(r, "credential_id")
	var revoked bool
	err := h.Database.WithWorkspaceTx(ctx, string(ws), "computer_hub.instance_credentials_delete", func(tx *dbconnect.Tx) error {
		var err error
		revoked, err = instance.RevokeCredential(ctx, tx, ws, instanceID, credentialID)
		return err
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	if revoked {
		instance.AuditCredentialOperation(ctx, h.logger, actorOf(principal), instanceID, credentialID, instance.AuditRevoke)
	}
	w.WriteHeader(http.StatusNoContent)
}

// issuance is the result of an issuing request. key is nil on a replay;
// object is the instance object of an instance creation, and live the
// credential of the instance's live connection.
type issuance struct {
	instance   instance.Instance
	object     instance.InstanceObject
	credential instance.Credential
	key        *instance.ConnectionKey
	live       string
	now        time.Time
}

func (i issuance) credentialObject() instance.IssuedCredentialObject {
	object := instance.IssuedCredentialObject{CredentialObject: instance.CredentialObjectOf(i.credential, i.live, i.now)}
	if i.key != nil {
		plaintext := i.key.Reveal()
		object.ConnectionKey = &plaintext
	}
	return object
}

// replay answers a request whose idempotency record exists: an equal
// fingerprint returns the current objects of the recorded identities without
// a key, any other request IdempotencyConflictError.
func (h *instanceHandler) replay(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, scope instance.IdempotencyScope, fingerprint [32]byte, canonical bool) (issuance, bool, error) {
	record, found, err := instance.FindIdempotencyRecord(ctx, tx, ws, scope)
	if err != nil || !found {
		return issuance{}, false, err
	}
	if !canonical || record.Fingerprint != fingerprint {
		return issuance{}, true, &instance.IdempotencyConflictError{}
	}
	current, err := instance.ReadInstance(ctx, tx, ws, record.InstanceID)
	if err != nil {
		return issuance{}, true, err
	}
	credential, err := instance.ReadCredential(ctx, tx, ws, record.CredentialID)
	if err != nil {
		return issuance{}, true, err
	}
	now, err := instance.Now(ctx, tx)
	if err != nil {
		return issuance{}, true, err
	}
	object, err := instance.ReadObject(ctx, tx, ws, current, h.RecommendedCLIVersion, now)
	if err != nil {
		return issuance{}, true, err
	}
	live, err := instance.LiveCredentialID(ctx, tx, ws, current.ID, now)
	return issuance{instance: current, object: object, credential: credential, live: live, now: now}, true, err
}

// idempotentTx runs fn in one transaction and runs it once more when a
// concurrent identical request committed the same idempotency record first;
// the second run finds that record.
func (h *instanceHandler) idempotentTx(ctx context.Context, ws workspace.ID, operation string, fn func(*dbconnect.Tx) error) error {
	err := h.Database.WithWorkspaceTx(ctx, string(ws), operation, fn)
	if instance.IsIdempotencyRace(err) {
		err = h.Database.WithWorkspaceTx(ctx, string(ws), operation, fn)
	}
	return err
}

func (h *instanceHandler) auditIssue(ctx context.Context, principal auth.Principal, issued issuance) {
	if issued.key != nil {
		instance.AuditCredentialOperation(ctx, h.logger, actorOf(principal), issued.instance.ID, issued.credential.ID, instance.AuditIssue)
	}
}

func (h *instanceHandler) expiryError() error {
	return &httpapi.ValidationError{Message: "expires_at must be an RFC 3339 UTC time after issuance and at most " + h.MaxCredentialLifetime.String() + " after it"}
}

// writeError answers the API envelope. A failure that is not a request or
// state error the routes name is a database failure: 503, nothing changed.
func (h *instanceHandler) writeError(w http.ResponseWriter, r *http.Request, err error) {
	var (
		validation  *httpapi.ValidationError
		tooLarge    *httpapi.RequestTooLargeError
		conflict    *instance.IdempotencyConflictError
		limit       *instance.CredentialLimitError
		offline     *instance.OfflineError
		unavailable *instance.UnavailableError
	)
	switch {
	case errors.Is(err, instance.ErrNotFound):
		err = &httpapi.NotFoundError{Message: "not found"}
	case errors.As(err, &validation), errors.As(err, &tooLarge), errors.As(err, &conflict), errors.As(err, &limit), errors.As(err, &offline), errors.As(err, &unavailable):
	default:
		err = &httpapi.UnavailableError{Message: "the instance store is unavailable"}
	}
	httpapi.WriteError(w, r, err)
}

// idempotencyKey returns the Idempotency-Key header, which must appear
// exactly once and hold, trimmed, 1 to 255 bytes.
func idempotencyKey(r *http.Request) (string, error) {
	values := r.Header.Values("Idempotency-Key")
	if len(values) != 1 {
		return "", &httpapi.ValidationError{Message: "Idempotency-Key must be sent exactly once"}
	}
	key := strings.TrimSpace(values[0])
	if key == "" || len(key) > 255 {
		return "", &httpapi.ValidationError{Message: "Idempotency-Key must hold 1 to 255 bytes"}
	}
	return key, nil
}

func invalidBody() error {
	return &httpapi.ValidationError{Message: "request body must be one JSON object without repeated keys"}
}

// decodeObject decodes a request body that Fingerprint accepted into its
// members and refuses any member outside allowed.
func decodeObject(body []byte, allowed ...string) (map[string]json.RawMessage, error) {
	return decodeObjectField(body, "request body", allowed...)
}

func decodeObjectField(raw json.RawMessage, name string, allowed ...string) (map[string]json.RawMessage, error) {
	if trimmed := bytes.TrimSpace(raw); len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, &httpapi.ValidationError{Message: name + " must be an object"}
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, &httpapi.ValidationError{Message: name + " must be an object"}
	}
	for field := range fields {
		known := false
		for _, candidate := range allowed {
			known = known || field == candidate
		}
		if !known {
			return nil, &httpapi.ValidationError{Message: name + " has an unknown field " + strconv.Quote(field)}
		}
	}
	return fields, nil
}

func instanceName(raw json.RawMessage) (string, error) {
	var name string
	if len(raw) == 0 || raw[0] != '"' || json.Unmarshal(raw, &name) != nil {
		return "", &httpapi.ValidationError{Message: "name must be a string of 1 to 256 characters"}
	}
	if count := utf8.RuneCountInString(name); count < 1 || count > maxInstanceNameLength {
		return "", &httpapi.ValidationError{Message: "name must be a string of 1 to 256 characters"}
	}
	return name, nil
}

// requestedExpiry reads expires_at: absent is nil; present must be an
// RFC 3339 time in UTC (ending in Z).
func requestedExpiry(fields map[string]json.RawMessage) (*time.Time, error) {
	raw, present := fields["expires_at"]
	if !present {
		return nil, nil
	}
	var text string
	if len(raw) == 0 || raw[0] != '"' || json.Unmarshal(raw, &text) != nil || !strings.HasSuffix(text, "Z") {
		return nil, &httpapi.ValidationError{Message: "expires_at must be an RFC 3339 UTC time"}
	}
	at, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return nil, &httpapi.ValidationError{Message: "expires_at must be an RFC 3339 UTC time"}
	}
	return &at, nil
}

func credentialDelivery(fields map[string]json.RawMessage) (string, error) {
	raw, present := fields["delivery"]
	if !present {
		return instance.DeliveryStream, nil
	}
	var delivery string
	if len(raw) == 0 || raw[0] != '"' || json.Unmarshal(raw, &delivery) != nil || (delivery != instance.DeliveryStream && delivery != instance.DeliveryResponse) {
		return "", &httpapi.ValidationError{Message: "delivery must be stream or response"}
	}
	return delivery, nil
}

func mustPrincipal(r *http.Request) auth.Principal {
	// The operation gate admitted the request, so the principal is present.
	principal, _ := auth.PrincipalFromContext(r.Context())
	return principal
}

func actorOf(principal auth.Principal) instance.Actor {
	return instance.Actor{CredentialKind: principal.Credential.Kind, CredentialID: principal.Credential.ID}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
