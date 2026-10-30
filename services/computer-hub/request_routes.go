package computerhub

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/hubflow"
	"github.com/tetral-ai/tetral/internal/instance"
)

const (
	routeRequests       = "/v1/instance_requests"
	routeRequestStream  = "/v1/instance_requests/stream"
	routeRequestClaim   = "/v1/instance_requests/{request_id}/claim"
	routeRequestRelease = "/v1/instance_requests/{request_id}/release"
	routeRequestFail    = "/v1/instance_requests/{request_id}/fail"
)

// claimResult is the body of a claim: the request, the bootstrap that
// `tetral configure --from-stdin` reads and, for a new request, the
// computer's working directory.
type claimResult struct {
	Request          computer.RequestObject `json:"request"`
	Bootstrap        bootstrap              `json:"bootstrap"`
	WorkingDirectory *string                `json:"working_directory,omitempty"`
}

type bootstrap struct {
	ServerURL     string  `json:"server_url"`
	InstanceID    string  `json:"instance_id"`
	CredentialID  string  `json:"credential_id"`
	ConnectionKey *string `json:"connection_key"`
	ExpiresAt     string  `json:"expires_at"`
}

// requestEnvironment reads the required environment_id of the request list
// and stream.
func requestEnvironment(r *http.Request) (string, error) {
	values := r.URL.Query()["environment_id"]
	if len(values) != 1 || values[0] == "" {
		return "", &httpapi.ValidationError{Message: "environment_id is required, once"}
	}
	return values[0], nil
}

func (h *instanceHandler) listRequests(w http.ResponseWriter, r *http.Request) {
	ctx, ws := r.Context(), mustPrincipal(r).Workspace.ID
	environmentID, err := requestEnvironment(r)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	if states := r.URL.Query()["state"]; len(states) > 1 || (len(states) == 1 && states[0] != computer.RequestPending) {
		h.writeError(w, r, &httpapi.ValidationError{Message: "state accepts only pending"})
		return
	}
	binding := pageBinding{WorkspaceID: string(ws), Route: http.MethodGet + " " + routeRequests,
		Filters: map[string]string{"environment_id": environmentID, "state": computer.RequestPending}, Order: pageOrder}
	page, err := parseListPage(r, h.PageTokenKey, binding)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	response := listResponse[computer.RequestObject]{Data: []computer.RequestObject{}}
	err = h.Database.WithWorkspaceReadOnlyTx(ctx, string(ws), "computer_hub.instance_requests_list", func(tx *dbconnect.Tx) error {
		rows, err := computer.ListPendingRequests(ctx, tx, ws, environmentID, page.after, page.limit+1)
		if err != nil {
			return err
		}
		more := len(rows) > page.limit
		rows = rows[:min(len(rows), page.limit)]
		for _, row := range rows {
			response.Data = append(response.Data, row.Object())
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

func (h *instanceHandler) claimRequest(w http.ResponseWriter, r *http.Request) {
	ctx, principal := r.Context(), mustPrincipal(r)
	ws := principal.Workspace.ID
	body, err := httpapi.ReadJSONBody(w, r)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	fields, err := decodeObject(body, "worker_id")
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	var workerID string
	if raw := fields["worker_id"]; len(raw) == 0 || raw[0] != '"' || json.Unmarshal(raw, &workerID) != nil || workerID == "" {
		h.writeError(w, r, &httpapi.ValidationError{Message: "worker_id must be a nonempty string"})
		return
	}
	var claim computer.Claim
	err = h.Database.WithWorkspaceTx(ctx, string(ws), "computer_hub.instance_requests_claim", func(tx *dbconnect.Tx) error {
		var err error
		claim, err = computer.ClaimRequest(ctx, tx, ws, chi.URLParam(r, "request_id"), workerID, h.MaxCredentialLifetime)
		return err
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	result := claimResult{Request: claim.Request.Object(), WorkingDirectory: claim.WorkingDirectory, Bootstrap: bootstrap{
		ServerURL: h.ServerURL, InstanceID: claim.Credential.InstanceID, CredentialID: claim.Credential.ID, ExpiresAt: instance.FormatTime(claim.Credential.ExpiresAt),
	}}
	if claim.Key != nil {
		plaintext := claim.Key.Reveal()
		result.Bootstrap.ConnectionKey = &plaintext
		actor := actorOf(principal)
		for _, credentialID := range claim.Revoked {
			instance.AuditCredentialOperation(ctx, h.logger, actor, claim.Credential.InstanceID, credentialID, instance.AuditRevoke)
		}
		instance.AuditCredentialOperation(ctx, h.logger, actor, claim.Credential.InstanceID, claim.Credential.ID, instance.AuditIssue)
		h.Metrics.requestClaimed(claim.Request.ClaimedAt.Sub(claim.Request.CreatedAt))
	}
	writeJSON(w, http.StatusOK, result)
}

// releaseRequest returns a claimed request to pending; a pending one answers
// unchanged.
func (h *instanceHandler) releaseRequest(w http.ResponseWriter, r *http.Request) {
	h.endClaim(w, r, "computer_hub.instance_requests_release", computer.RequestPending, func(tx *dbconnect.Tx, requestID string) (string, error) {
		return hubflow.ReturnRequestToPending(r.Context(), tx, mustPrincipal(r).Workspace.ID, requestID)
	})
}

// failRequest ends a claimed request as failed; a failed one answers
// unchanged.
func (h *instanceHandler) failRequest(w http.ResponseWriter, r *http.Request) {
	h.endClaim(w, r, "computer_hub.instance_requests_fail", computer.RequestFailed, func(tx *dbconnect.Tx, requestID string) (string, error) {
		return hubflow.FailRequest(r.Context(), tx, mustPrincipal(r).Workspace.ID, requestID, false)
	})
}

// endClaim answers release and fail: a claimed request goes through end; a
// request already in the state end leads to answers unchanged; any other
// state answers RequestNotClaimedError.
func (h *instanceHandler) endClaim(w http.ResponseWriter, r *http.Request, operation, unchanged string, end func(*dbconnect.Tx, string) (string, error)) {
	ctx, principal := r.Context(), mustPrincipal(r)
	ws, requestID := principal.Workspace.ID, chi.URLParam(r, "request_id")
	var request computer.Request
	var revoked string
	var ended bool
	err := h.Database.WithWorkspaceTx(ctx, string(ws), operation, func(tx *dbconnect.Tx) error {
		current, err := computer.LockRequest(ctx, tx, ws, requestID)
		if err != nil {
			return err
		}
		revoked, ended = "", false
		switch current.State {
		case unchanged:
			request = current
			return nil
		case computer.RequestClaimed:
		default:
			return &computer.RequestNotClaimedError{}
		}
		if revoked, err = end(tx, requestID); err != nil {
			return err
		}
		ended = true
		request, err = computer.ReadRequest(ctx, tx, ws, requestID)
		return err
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	if revoked != "" {
		instance.AuditCredentialOperation(ctx, h.logger, actorOf(principal), *request.InstanceID, revoked, instance.AuditRevoke)
	}
	if ended && request.State == computer.RequestFailed {
		h.Metrics.requestFailed(ctx)
	}
	writeJSON(w, http.StatusOK, request.Object())
}
