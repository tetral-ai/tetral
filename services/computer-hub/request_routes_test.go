package computerhub

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/enginetest"
	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/hubflow"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	"github.com/tetral-ai/tetral/internal/workload"
)

// requestServerURL is the server URL the fixture's hub servers put in a
// claim's bootstrap.
const requestServerURL = "https://api.tetral.test"

// claimBody is a claim result as Controllers read it.
type claimBody struct {
	Request   map[string]any `json:"request"`
	Bootstrap struct {
		ServerURL     string  `json:"server_url"`
		InstanceID    string  `json:"instance_id"`
		CredentialID  string  `json:"credential_id"`
		ConnectionKey *string `json:"connection_key"`
		ExpiresAt     string  `json:"expires_at"`
	} `json:"bootstrap"`
	WorkingDirectory *string `json:"working_directory"`
}

// cloudComputer seeds an idle Session and its attached cloud computer
// comp_<sessionID> in the Session's Environment env_<sessionID>, working in
// /workspace. With current, the computer's current instance is a new active
// cloud instance holding one active credential, whose CLI it returns.
func (f *hubFixture) cloudComputer(sessionID string, current bool) *enginetest.CLI {
	f.t.Helper()
	sessionfixture.SeedBridgeAPISession(f.t, f.admin, string(f.ws), sessionID, "sthr_"+sessionID)
	if _, err := f.admin.ExecContext(f.ctx, `INSERT INTO computers
		(workspace_id,session_id,id,kind,environment_id,working_directory,networking,drive_exclude,association_state,execution_state)
		VALUES ($1,$2,$3,'cloud',$4,'/workspace','{"type":"unrestricted"}','[]','attached','waiting')`,
		string(f.ws), sessionID, "comp_"+sessionID, "env_"+sessionID); err != nil {
		f.t.Fatal(err)
	}
	if !current {
		return nil
	}
	var cli *enginetest.CLI
	f.ownerTx(func(tx *dbconnect.Tx) error {
		created, err := instance.CreateCloudInstance(f.ctx, tx, f.ws, "comp_"+sessionID, sessionID)
		if err != nil {
			return err
		}
		now, err := instance.Now(f.ctx, tx)
		if err != nil {
			return err
		}
		credential, _, err := instance.IssueClaimCredential(f.ctx, tx, f.ws, created.ID, instance.CredentialActive, now, now.Add(f.settings.maxLifetime))
		if err != nil {
			return err
		}
		cli = f.cli(created.ID, credential.ID)
		_, err = tx.Exec(f.ctx, `UPDATE computers SET current_instance_id=$3 WHERE workspace_id=$1 AND id=$2`, string(f.ws), "comp_"+sessionID, created.ID)
		return err
	})
	return cli
}

// ownerTx runs fn in a Workspace transaction as the table owner, for what no
// hub role writes.
func (f *hubFixture) ownerTx(fn func(*dbconnect.Tx) error) {
	f.t.Helper()
	if err := f.owner.WithWorkspaceTx(f.ctx, string(f.ws), "test.request_setup", fn); err != nil {
		f.t.Fatal(err)
	}
}

// hubTx runs fn in a Workspace transaction as computer_hub.
func (f *hubFixture) hubTx(fn func(*dbconnect.Tx) error) {
	f.t.Helper()
	if err := f.client.WithWorkspaceTx(f.ctx, string(f.ws), "test.request_writer", fn); err != nil {
		f.t.Fatal(err)
	}
}

// ensureRequest records the open request of the Session's cloud computer, as
// tool-call admission does.
func (f *hubFixture) ensureRequest(sessionID string) computer.Request {
	f.t.Helper()
	var request computer.Request
	f.ownerTx(func(tx *dbconnect.Tx) error {
		var err error
		request, err = computer.EnsureRequest(f.ctx, tx, f.ws, sessionID, "comp_"+sessionID)
		return err
	})
	return request
}

func (f *hubFixture) requestRoute(requestID, action, body string) (int, []byte) {
	f.t.Helper()
	return f.do(http.MethodPost, "/v1/instance_requests/"+requestID+"/"+action, nil, body)
}

// claim claims requestID for workerID, which must answer 200.
func (f *hubFixture) claim(requestID, workerID string) claimBody {
	f.t.Helper()
	status, data := f.requestRoute(requestID, "claim", `{"worker_id":"`+workerID+`"}`)
	if status != http.StatusOK {
		f.t.Fatalf("claim of %s by %s = %d %s", requestID, workerID, status, data)
	}
	var body claimBody
	decodeJSON(f.t, data, &body)
	return body
}

// requestObject answers action on requestID, which must be 200, and returns
// the request object.
func (f *hubFixture) requestObject(requestID, action string) map[string]any {
	f.t.Helper()
	status, data := f.requestRoute(requestID, action, "")
	if status != http.StatusOK {
		f.t.Fatalf("%s of %s = %d %s", action, requestID, status, data)
	}
	var object map[string]any
	decodeJSON(f.t, data, &object)
	return object
}

// requestRow reads a computer_requests row through the admin pool.
func (f *hubFixture) requestRow(requestID string) map[string]any {
	f.t.Helper()
	var data []byte
	if err := f.admin.QueryRowContext(f.ctx, `SELECT row_to_json(r) FROM computer_requests r WHERE id=$1`, requestID).Scan(&data); err != nil {
		f.t.Fatal(err)
	}
	var row map[string]any
	decodeJSON(f.t, data, &row)
	return row
}

// TestOneWorkerWinsTheClaimOfARequest claims a new and a wake request from
// two workers at once while the first claim holds the row: the first wins
// and the other answers 409 request_already_claimed_error. The winner's
// repeated claim returns the same IDs without a key. After release, the next
// claim of the new request creates another instance with another key, and
// the next claim of the wake request revokes the first claim's never-used
// credential with cause request_ended and issues another for the same
// instance.
func TestOneWorkerWinsTheClaimOfARequest(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	for _, kind := range []string{computer.RequestNew, computer.RequestWake} {
		sessionID := "sesn_claim_" + kind
		current := f.cloudComputer(sessionID, kind == computer.RequestWake)
		request := f.ensureRequest(sessionID)
		if request.Kind != kind || request.State != computer.RequestPending || request.EnvironmentID != "env_"+sessionID {
			t.Fatalf("recorded request %+v; want a pending %s request of env_%s", request, kind, sessionID)
		}

		pause := f.pauseOn("UPDATE computer_requests SET state='claimed'")
		type answer struct {
			status int
			data   []byte
		}
		first, second := make(chan answer, 1), make(chan answer, 1)
		go func() {
			status, data := f.requestRoute(request.ID, "claim", `{"worker_id":"worker_a"}`)
			first <- answer{status, data}
		}()
		<-pause.reached
		go func() {
			status, data := f.requestRoute(request.ID, "claim", `{"worker_id":"worker_b"}`)
			second <- answer{status, data}
		}()
		f.awaitLockWait()
		close(pause.release)
		won, lost := <-first, <-second
		requireAPIError(t, lost.status, lost.data, http.StatusConflict, "request_already_claimed_error", "")
		if won.status != http.StatusOK {
			t.Fatalf("first claim = %d %s", won.status, won.data)
		}
		var winner claimBody
		decodeJSON(t, won.data, &winner)
		var fields map[string]json.RawMessage
		decodeJSON(t, won.data, &fields)
		_, hasDirectory := fields["working_directory"]
		if winner.Request["state"] != computer.RequestClaimed || winner.Request["worker_id"] != "worker_a" || winner.Request["instance_id"] != winner.Bootstrap.InstanceID ||
			winner.Bootstrap.ServerURL != requestServerURL || winner.Bootstrap.ConnectionKey == nil || hasDirectory != (kind == computer.RequestNew) {
			t.Fatalf("claim result %s; want worker_a's claim with a key, server_url %s and a working directory only for new", won.data, requestServerURL)
		}
		credential := f.credentialRow(winner.Bootstrap.CredentialID)
		wantState := map[string]string{computer.RequestNew: instance.CredentialActive, computer.RequestWake: instance.CredentialPending}[kind]
		if credential["state"] != wantState || credential["delivery"] != instance.DeliveryClaim || credential["instance_id"] != winner.Bootstrap.InstanceID {
			t.Fatalf("claim credential %v; want a %s claim credential of the claim's instance", credential, wantState)
		}
		claimed := f.instanceRead(winner.Bootstrap.InstanceID)
		if kind == computer.RequestNew {
			if *winner.WorkingDirectory != "/workspace" || claimed["kind"] != instance.KindCloud || claimed["computer_id"] != "comp_"+sessionID || claimed["name"] != nil || claimed["registration_state"] != "active" {
				t.Fatalf("new claim's instance %v, working directory %v; want an active unnamed cloud instance of comp_%s and /workspace", claimed, *winner.WorkingDirectory, sessionID)
			}
		} else if winner.Bootstrap.InstanceID != current.InstanceID {
			t.Fatalf("wake claim for instance %s; want the current instance %s", winner.Bootstrap.InstanceID, current.InstanceID)
		}

		repeated := f.claim(request.ID, "worker_a")
		if repeated.Request["id"] != request.ID || repeated.Bootstrap.InstanceID != winner.Bootstrap.InstanceID ||
			repeated.Bootstrap.CredentialID != winner.Bootstrap.CredentialID || repeated.Bootstrap.ConnectionKey != nil {
			t.Fatalf("repeated claim %+v; want the same request, instance and credential and connection_key null", repeated)
		}
		if released := f.requestObject(request.ID, "release"); released["state"] != computer.RequestPending || released["worker_id"] != "worker_a" {
			t.Fatalf("release = %v; want pending, keeping worker_a", released)
		}
		next := f.claim(request.ID, "worker_b")
		if next.Bootstrap.ConnectionKey == nil || *next.Bootstrap.ConnectionKey == *winner.Bootstrap.ConnectionKey || next.Bootstrap.CredentialID == winner.Bootstrap.CredentialID {
			t.Fatalf("claim after release %+v; want a new credential and key", next)
		}
		switch kind {
		case computer.RequestNew:
			if next.Bootstrap.InstanceID == winner.Bootstrap.InstanceID {
				t.Fatalf("claim after release reused instance %s; want another", next.Bootstrap.InstanceID)
			}
		case computer.RequestWake:
			revoked := f.credentialRow(winner.Bootstrap.CredentialID)
			if next.Bootstrap.InstanceID != current.InstanceID || revoked["state"] != "revoked" || revoked["revocation_cause"] != instance.CauseRequestEnded {
				t.Fatalf("claim after release for %s left the first claim's credential %v; want the same instance and it revoked with request_ended", next.Bootstrap.InstanceID, revoked)
			}
		}
	}
}

// TestRequestRoutesAnswerEveryState sends claim, release and fail to a
// request in each state, one row per answer of the state table that the
// claim test does not reach. A refused route leaves the request as it was.
func TestRequestRoutesAnswerEveryState(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	// into returns a new request brought into state by the writers that
	// reach it.
	into := func(name, state string) string {
		sessionID := "sesn_state_" + name
		f.cloudComputer(sessionID, false)
		request := f.ensureRequest(sessionID)
		if state == computer.RequestPending || state == computer.RequestExpired {
			if state == computer.RequestExpired {
				f.hubTx(func(tx *dbconnect.Tx) error { return computer.Expire(f.ctx, tx, f.ws, request.ID) })
			}
			return request.ID
		}
		claimed := f.claim(request.ID, "worker_a")
		switch state {
		case computer.RequestFulfilled:
			f.hubTx(func(tx *dbconnect.Tx) error {
				return computer.FulfilRequests(f.ctx, tx, f.ws, claimed.Bootstrap.InstanceID)
			})
		case computer.RequestFailed:
			f.requestObject(request.ID, "fail")
		}
		return request.ID
	}
	for i, row := range []struct {
		route, state string
		status       int
		errorType    string
		becomes      string
	}{
		{"claim", computer.RequestFulfilled, http.StatusConflict, "request_not_pending_error", ""},
		{"claim", computer.RequestFailed, http.StatusConflict, "request_not_pending_error", ""},
		{"claim", computer.RequestExpired, http.StatusConflict, "request_not_pending_error", ""},
		{"release", computer.RequestPending, http.StatusOK, "", computer.RequestPending},
		{"release", computer.RequestFulfilled, http.StatusConflict, "request_not_claimed_error", ""},
		{"release", computer.RequestFailed, http.StatusConflict, "request_not_claimed_error", ""},
		{"release", computer.RequestExpired, http.StatusConflict, "request_not_claimed_error", ""},
		{"fail", computer.RequestPending, http.StatusConflict, "request_not_claimed_error", ""},
		{"fail", computer.RequestClaimed, http.StatusOK, "", computer.RequestFailed},
		{"fail", computer.RequestFulfilled, http.StatusConflict, "request_not_claimed_error", ""},
		{"fail", computer.RequestFailed, http.StatusOK, "", computer.RequestFailed},
		{"fail", computer.RequestExpired, http.StatusConflict, "request_not_claimed_error", ""},
	} {
		requestID := into(fmt.Sprint(i), row.state)
		before := f.requestRow(requestID)
		body := ""
		if row.route == "claim" {
			body = `{"worker_id":"worker_b"}`
		}
		status, data := f.requestRoute(requestID, row.route, body)
		after := f.requestRow(requestID)
		if row.status != http.StatusOK {
			requireAPIError(t, status, data, row.status, row.errorType, "")
			if fmt.Sprint(after) != fmt.Sprint(before) {
				t.Fatalf("%s of a %s request changed it from %v to %v", row.route, row.state, before, after)
			}
			continue
		}
		var object map[string]any
		decodeJSON(t, data, &object)
		if status != http.StatusOK || object["id"] != requestID || object["state"] != row.becomes || after["state"] != row.becomes {
			t.Fatalf("%s of a %s request = %d %s; want 200 with the request %s", row.route, row.state, status, data, row.becomes)
		}
		if row.becomes == row.state && fmt.Sprint(after) != fmt.Sprint(before) {
			t.Fatalf("%s of a %s request changed it from %v to %v; want it unchanged", row.route, row.state, before, after)
		}
	}
}

// TestWakeClaimAtTheCredentialLimitChangesNothing claims a wake request
// whose instance holds two active credentials and a never-used pending one:
// 409 credential_limit_error, the pending credential stays pending, nothing
// is issued and the request stays pending.
func TestWakeClaimAtTheCredentialLimitChangesNothing(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	current := f.cloudComputer("sesn_wake_limit", true)
	// The issuance path refuses a third usable credential, so the second
	// active and the pending one are written directly.
	for id, state := range map[string]string{"ccred_limit_active": instance.CredentialActive, "ccred_limit_pending": instance.CredentialPending} {
		if _, err := f.admin.ExecContext(f.ctx, `INSERT INTO instance_credentials (workspace_id,instance_id,id,state,key_digest,expires_at,delivery)
			VALUES ($1,$2,$3,$4,$5,clock_timestamp()+interval '1 day','claim')`, string(f.ws), current.InstanceID, id, state, instance.GenerateKey().Digest()); err != nil {
			t.Fatal(err)
		}
	}
	request := f.ensureRequest("sesn_wake_limit")
	status, data := f.requestRoute(request.ID, "claim", `{"worker_id":"worker_a"}`)
	requireAPIError(t, status, data, http.StatusConflict, "credential_limit_error", "")
	if credentials := f.count(`SELECT count(*) FROM instance_credentials WHERE instance_id=$1`, current.InstanceID); credentials != 3 {
		t.Fatalf("instance holds %d credentials after the refused claim; want the 3 it had", credentials)
	}
	if pending := f.credentialRow("ccred_limit_pending"); pending["state"] != instance.CredentialPending || pending["revoked_at"] != nil {
		t.Fatalf("pending credential %v after the refused claim; want it still pending", pending)
	}
	if row := f.requestRow(request.ID); row["state"] != computer.RequestPending || row["worker_id"] != nil || row["claim_credential_id"] != nil {
		t.Fatalf("request %v after the refused claim; want it pending and unclaimed", row)
	}
}

// TestReturnedNewClaimRetiresItsInstance returns a claimed new request to
// pending as the claim timeout does: the claim's instance becomes unused with
// one unused event, its credential is revoked with cause new_claim_returned,
// the request keeps the claim's worker and instance, and the next claim
// creates another instance.
func TestReturnedNewClaimRetiresItsInstance(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	events := f.instanceEvents()
	f.cloudComputer("sesn_returned", false)
	request := f.ensureRequest("sesn_returned")
	first := f.claim(request.ID, "worker_a")
	if unused := f.instanceRead(first.Bootstrap.InstanceID)["unused"]; unused != false {
		t.Fatalf("claim instance unused = %v while claimed; want false", unused)
	}
	var revoked string
	f.hubTx(func(tx *dbconnect.Tx) error {
		var err error
		revoked, err = hubflow.ReturnRequestToPending(f.ctx, tx, f.ws, request.ID)
		return err
	})
	credential := f.credentialRow(first.Bootstrap.CredentialID)
	if revoked != first.Bootstrap.CredentialID || credential["state"] != "revoked" || credential["revocation_cause"] != instance.CauseNewClaimReturned {
		t.Fatalf("return revoked %q, leaving the claim credential %v; want it revoked with new_claim_returned", revoked, credential)
	}
	if unused := f.instanceRead(first.Bootstrap.InstanceID)["unused"]; unused != true {
		t.Fatalf("claim instance unused = %v after the return; want true", unused)
	}
	if event := events.next(t); event.name != instance.EventUnused || event.instanceID != first.Bootstrap.InstanceID {
		t.Fatalf("instance event %+v; want unused of %s", event, first.Bootstrap.InstanceID)
	}
	events.none(t)
	if row := f.requestRow(request.ID); row["state"] != computer.RequestPending || row["worker_id"] != "worker_a" || row["instance_id"] != first.Bootstrap.InstanceID {
		t.Fatalf("returned request %v; want pending with the claim's worker and instance", row)
	}
	if next := f.claim(request.ID, "worker_b"); next.Bootstrap.InstanceID == first.Bootstrap.InstanceID {
		t.Fatalf("claim after the return reused instance %s; want another", next.Bootstrap.InstanceID)
	}
}

// TestExpiredNewRequestMakesWayForTheNext expires a pending new request as
// the pending expiry does: it is expired, and the computer's next request is
// another pending new request.
func TestExpiredNewRequestMakesWayForTheNext(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	f.cloudComputer("sesn_expired", false)
	request := f.ensureRequest("sesn_expired")
	f.hubTx(func(tx *dbconnect.Tx) error { return computer.Expire(f.ctx, tx, f.ws, request.ID) })
	if state := f.requestRow(request.ID)["state"]; state != computer.RequestExpired {
		t.Fatalf("request state %v after the expiry; want expired", state)
	}
	if next := f.ensureRequest("sesn_expired"); next.ID == request.ID || next.Kind != computer.RequestNew || next.State != computer.RequestPending {
		t.Fatalf("next request %+v; want another pending new request", next)
	}
}

// TestRefusedReadyLeavesTheClaimUntilTheControllerFails sends the ready of a
// claim's CLI below the minimum CLI version: the refusal fulfils nothing and
// the request stays claimed. A Controller that reads the refusal on another
// claim's instance and fails that request leaves it failed, its instance
// unused, its credential revoked with cause request_ended and the CLI's
// connection ended as revoked.
func TestRefusedReadyLeavesTheClaimUntilTheControllerFails(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour, withCLIVersions("2.0.0", "2.0.0"))
	refused := func(sessionID string) (string, claimBody, *testStream) {
		f.cloudComputer(sessionID, false)
		request := f.ensureRequest(sessionID)
		claimed := f.claim(request.ID, "worker_a")
		cli := f.cli(claimed.Bootstrap.InstanceID, claimed.Bootstrap.CredentialID)
		stream := f.open(cli)
		response, err := cli.Ready(f.ctx, readyOf(stream.connectionID, "ready_1", "1.0.0"))
		requireClientError(t, response, err, protocol.ErrorCLIUpgradeRequired)
		return request.ID, claimed, stream
	}
	waiting, _, _ := refused("sesn_refused_waiting")
	if state := f.requestRow(waiting)["state"]; state != computer.RequestClaimed {
		t.Fatalf("request state %v after a refused ready; want claimed", state)
	}

	failed, claimed, stream := refused("sesn_refused_failed")
	connection, _ := f.instanceRead(claimed.Bootstrap.InstanceID)["connection"].(map[string]any)
	if refusal, _ := connection["error"].(map[string]any); refusal["code"] != string(protocol.ErrorCLIUpgradeRequired) {
		t.Fatalf("claim instance connection %v; want the cli_upgrade_required refusal", connection)
	}
	if object := f.requestObject(failed, "fail"); object["state"] != computer.RequestFailed {
		t.Fatalf("fail = %v; want failed", object)
	}
	credential := f.credentialRow(claimed.Bootstrap.CredentialID)
	if unused := f.instanceRead(claimed.Bootstrap.InstanceID)["unused"]; unused != true || credential["state"] != "revoked" || credential["revocation_cause"] != instance.CauseRequestEnded {
		t.Fatalf("after fail: instance unused %v, claim credential %v; want unused and revoked with request_ended", unused, credential)
	}
	if end := f.connectionRow(stream.connectionID)["end_cause"]; end != instance.EndRevoked {
		t.Fatalf("claim connection end cause %v; want revoked", end)
	}
}

// TestAcceptedReadyFulfilsTheRequestOfItsInstance accepts the ready of a
// pending wake request's instance and of a claimed new request's instance:
// each request is fulfilled, the new request's instance becomes its
// computer's current instance and is not unused, and a later claim of the
// wake request answers 409 request_not_pending_error.
func TestAcceptedReadyFulfilsTheRequestOfItsInstance(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	current := f.cloudComputer("sesn_fulfil_wake", true)
	wake := f.ensureRequest("sesn_fulfil_wake")
	f.acceptReady(current, f.open(current), "ready_wake")
	if state := f.requestRow(wake.ID)["state"]; state != computer.RequestFulfilled {
		t.Fatalf("wake request state %v after its instance's ready; want fulfilled", state)
	}
	status, data := f.requestRoute(wake.ID, "claim", `{"worker_id":"worker_a"}`)
	requireAPIError(t, status, data, http.StatusConflict, "request_not_pending_error", "")

	f.cloudComputer("sesn_fulfil_new", false)
	created := f.ensureRequest("sesn_fulfil_new")
	claimed := f.claim(created.ID, "worker_a")
	cli := f.cli(claimed.Bootstrap.InstanceID, claimed.Bootstrap.CredentialID)
	f.acceptReady(cli, f.open(cli), "ready_new")
	var currentInstance string
	if err := f.admin.QueryRowContext(f.ctx, `SELECT current_instance_id FROM computers WHERE id='comp_sesn_fulfil_new'`).Scan(&currentInstance); err != nil {
		t.Fatal(err)
	}
	if state := f.requestRow(created.ID)["state"]; state != computer.RequestFulfilled || currentInstance != claimed.Bootstrap.InstanceID ||
		f.instanceRead(claimed.Bootstrap.InstanceID)["unused"] != false {
		t.Fatalf("new request %v, computer current instance %s; want fulfilled with the claim's instance %s current and not unused", state, currentInstance, claimed.Bootstrap.InstanceID)
	}
}

// TestOnlyUsesOfACredentialAWakeClaimReplacedAreRecorded uses the credential
// revoked when a new claim returned to pending: 401 credential_revoked and
// nothing recorded. Then it uses, after the first use of a wake claim's
// credential, the instance credential that first use revoked: 401
// credential_revoked, last_credential_reuse names it and one credential_reused
// event follows.
func TestOnlyUsesOfACredentialAWakeClaimReplacedAreRecorded(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	events := f.instanceEvents()
	f.cloudComputer("sesn_reuse_new", false)
	returned := f.ensureRequest("sesn_reuse_new")
	claimed := f.claim(returned.ID, "worker_a")
	f.hubTx(func(tx *dbconnect.Tx) error {
		_, err := hubflow.ReturnRequestToPending(f.ctx, tx, f.ws, returned.ID)
		return err
	})
	if event := events.next(t); event.name != instance.EventUnused {
		t.Fatalf("instance event %+v; want the returned claim's unused", event)
	}
	stale := f.cli(claimed.Bootstrap.InstanceID, claimed.Bootstrap.CredentialID)
	_, response, err := stale.OpenEvents(f.ctx, stale.EventsQuery("", nil))
	requireClientError(t, response, err, protocol.ErrorCredentialRevoked)
	if reuse := f.instanceRead(claimed.Bootstrap.InstanceID)["last_credential_reuse"]; reuse != nil {
		t.Fatalf("a use of a returned claim's credential recorded %v", reuse)
	}
	events.none(t)

	current := f.cloudComputer("sesn_reuse_wake", true)
	wake := f.claim(f.ensureRequest("sesn_reuse_wake").ID, "worker_a")
	f.open(f.cli(current.InstanceID, wake.Bootstrap.CredentialID))
	if replaced := f.credentialRow(current.CredentialID); replaced["state"] != "revoked" || replaced["revocation_cause"] != "pending_activation" {
		t.Fatalf("instance credential %v after the wake credential's first use; want it revoked with pending_activation", replaced)
	}
	_, response, err = current.OpenEvents(f.ctx, current.EventsQuery("", nil))
	requireClientError(t, response, err, protocol.ErrorCredentialRevoked)
	reuse, _ := f.instanceRead(current.InstanceID)["last_credential_reuse"].(map[string]any)
	if reuse == nil || reuse["credential_id"] != current.CredentialID {
		t.Fatalf("last_credential_reuse = %v; want the replaced credential %s", reuse, current.CredentialID)
	}
	if event := events.next(t); event.name != "credential_reused" || event.instanceID != current.InstanceID {
		t.Fatalf("instance event %+v; want credential_reused of %s", event, current.InstanceID)
	}
	events.none(t)
}

// instanceEvents subscribes to the fixture Workspace's instance stream once
// its listener listens.
func (f *hubFixture) instanceEvents() streamedEvents {
	f.t.Helper()
	select {
	case <-f.events.listening:
	case <-f.ctx.Done():
		f.t.Fatal("the instance event listener never listened")
	}
	return streamedEventsOf(f.t, f.openStream(f.principal))
}

// TestRequestRoutesRefuseMalformedInput sends the list, stream and claim
// requests the routes refuse.
func TestRequestRoutesRefuseMalformedInput(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	f.cloudComputer("sesn_malformed", false)
	request := f.ensureRequest("sesn_malformed")
	for _, row := range []struct {
		name, method, target, body string
		status                     int
		errorType, naming          string
	}{
		{"list without environment_id", http.MethodGet, "/v1/instance_requests", "", http.StatusBadRequest, "invalid_request_error", "environment_id"},
		{"list of claimed requests", http.MethodGet, "/v1/instance_requests?environment_id=env_sesn_malformed&state=claimed", "", http.StatusBadRequest, "invalid_request_error", "state"},
		{"stream without environment_id", http.MethodGet, "/v1/instance_requests/stream", "", http.StatusBadRequest, "invalid_request_error", "environment_id"},
		{"claim with an empty worker_id", http.MethodPost, "/v1/instance_requests/" + request.ID + "/claim", `{"worker_id":""}`, http.StatusBadRequest, "invalid_request_error", "worker_id"},
		{"claim of an unknown request", http.MethodPost, "/v1/instance_requests/ireq_unknown/claim", `{"worker_id":"worker_a"}`, http.StatusNotFound, "not_found_error", ""},
	} {
		status, data := f.do(row.method, row.target, nil, row.body)
		requireAPIError(t, status, data, row.status, row.errorType, row.naming)
	}
}

// TestRequestListPagesAnEnvironmentsPendingRequests lists one Environment's
// requests a page of one at a time: only its pending requests, in creation
// order, with the Environment bound into the page token.
func TestRequestListPagesAnEnvironmentsPendingRequests(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	// Three computers share the Environment env_shared; another
	// Environment's pending request stays out of the list.
	var requests []computer.Request
	for _, sessionID := range []string{"sesn_list_a", "sesn_list_b", "sesn_list_claimed"} {
		f.cloudComputer(sessionID, false)
		if _, err := f.admin.ExecContext(f.ctx, `UPDATE computers SET environment_id='env_shared' WHERE id=$1`, "comp_"+sessionID); err != nil {
			t.Fatal(err)
		}
		requests = append(requests, f.ensureRequest(sessionID))
	}
	f.claim(requests[2].ID, "worker_a")
	f.cloudComputer("sesn_list_other", false)
	f.ensureRequest("sesn_list_other")

	var listed []string
	page := ""
	for range 3 {
		status, data := f.do(http.MethodGet, "/v1/instance_requests?environment_id=env_shared&state=pending&limit=1&page="+url.QueryEscape(page), nil, "")
		var body struct {
			Data     []map[string]any `json:"data"`
			NextPage *string          `json:"next_page"`
		}
		if status != http.StatusOK {
			t.Fatalf("list = %d %s", status, data)
		}
		decodeJSON(t, data, &body)
		for _, object := range body.Data {
			listed = append(listed, object["id"].(string))
		}
		if body.NextPage == nil {
			break
		}
		page = *body.NextPage
		status, data = f.do(http.MethodGet, "/v1/instance_requests?environment_id=env_sesn_list_other&limit=1&page="+url.QueryEscape(page), nil, "")
		requireAPIError(t, status, data, http.StatusBadRequest, "invalid_request_error", "page")
	}
	if fmt.Sprint(listed) != fmt.Sprint([]string{requests[0].ID, requests[1].ID}) {
		t.Fatalf("listed %v; want the two pending requests %s and %s in order", listed, requests[0].ID, requests[1].ID)
	}
}

// openRequestStream opens the request stream of environmentID on the hub
// server at serverURL.
func (f *hubFixture) openRequestStream(serverURL, environmentID string) *bufio.Reader {
	f.t.Helper()
	token, err := f.signer.Mint(f.principal, http.MethodGet, "/v1/instance_requests/stream", "req_hub_request_stream", time.Minute)
	if err != nil {
		f.t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(f.ctx, http.MethodGet, serverURL+"/v1/instance_requests/stream?environment_id="+environmentID, nil)
	if err != nil {
		f.t.Fatal(err)
	}
	request.Header.Set("X-Tetral-Internal-Principal", token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { _ = response.Body.Close() })
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream" {
		f.t.Fatalf("request stream = %d %s", response.StatusCode, response.Header.Get("Content-Type"))
	}
	return bufio.NewReader(response.Body)
}

// TestRequestStreamCarriesItsEnvironmentsEvents subscribes on one hub server
// to one Environment's request stream while requests change through another:
// the subscriber reads created, claimed, pending and expired of that
// Environment's request, each with the request object, and nothing of
// another Environment's request created first.
func TestRequestStreamCarriesItsEnvironmentsEvents(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	other := f.addPod("computer-hub-test-1")
	stream := f.openRequestStream(other.url, "env_sesn_stream")
	f.cloudComputer("sesn_stream_other", false)
	f.ensureRequest("sesn_stream_other")
	f.cloudComputer("sesn_stream", false)
	request := f.ensureRequest("sesn_stream")
	f.claim(request.ID, "worker_a")
	f.requestObject(request.ID, "release")
	f.hubTx(func(tx *dbconnect.Tx) error { return computer.Expire(f.ctx, tx, f.ws, request.ID) })
	for _, want := range []string{computer.RequestEventCreated, computer.RequestEventClaimed, computer.RequestEventPending, computer.RequestEventExpired} {
		name, data := readServerSentEvent(t, stream)
		var object map[string]any
		decodeJSON(t, []byte(data), &object)
		if name != want || object["id"] != request.ID || object["environment_id"] != "env_sesn_stream" {
			t.Fatalf("request event %s %s; want %s of %s", name, data, want, request.ID)
		}
	}
}

// TestClaimObservesTheRequestQueueWait claims a request created 2 seconds
// earlier by the database clock: the claiming hub server observes one queue
// wait of 2 seconds, plus the claim's own time.
func TestClaimObservesTheRequestQueueWait(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	f.cloudComputer("sesn_queue_wait", false)
	request := f.ensureRequest("sesn_queue_wait")
	if _, err := f.admin.ExecContext(f.ctx, `UPDATE computer_requests SET created_at=clock_timestamp()-interval '2 seconds' WHERE id=$1`, request.ID); err != nil {
		t.Fatal(err)
	}
	f.claim(request.ID, "worker_a")
	metrics := f.pods[hubTestPod].metrics
	count := metricValue(t, metrics, "computerhub_controller_requests_queue_wait_seconds_count", "")
	sum := metricValue(t, metrics, "computerhub_controller_requests_queue_wait_seconds_sum", "")
	if count != 1 || sum < 2 || sum >= 2.5 {
		t.Fatalf("queue wait observed %v times, %v seconds in all; want once, 2 seconds", count, sum)
	}
}

// TestEveryHubReadsTheSameRequestPressure reads the request pressure through
// two hub servers' pools while two requests are pending, the older for 30
// seconds, one is claimed and one has failed: each reads queued 2,
// in_progress 1 and an oldest age of 30 seconds.
func TestEveryHubReadsTheSameRequestPressure(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	var requests []computer.Request
	for _, sessionID := range []string{"sesn_pressure_old", "sesn_pressure_new", "sesn_pressure_claimed", "sesn_pressure_failed"} {
		f.cloudComputer(sessionID, false)
		requests = append(requests, f.ensureRequest(sessionID))
	}
	f.claim(requests[2].ID, "worker_a")
	f.claim(requests[3].ID, "worker_a")
	f.requestObject(requests[3].ID, "fail")
	if _, err := f.admin.ExecContext(f.ctx, `UPDATE computer_requests SET created_at=clock_timestamp()-interval '30 seconds' WHERE id=$1`, requests[0].ID); err != nil {
		t.Fatal(err)
	}
	second := dbconnect.NewClientForTesting(storagetest.OpenRuntimeRoleDBWithTracer(t, f.hubDB, nil))
	for i, database := range []*dbconnect.Client{f.client, second} {
		metrics, err := requestPressure(database)(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		values := map[string]float64{}
		for _, metric := range metrics {
			values[metric.Name] = metric.Value
		}
		if values["computerhub_controller_requests_queued"] != 2 || values["computerhub_controller_requests_in_progress"] != 1 ||
			math.Abs(values["computerhub_controller_requests_oldest_queued_seconds"]-30) > 1 {
			t.Fatalf("hub server %d read pressure %v; want two queued, the oldest 30 seconds within a second, and one in progress", i, values)
		}
	}
}

// TestControllerFailCountsOnceAtTheRequestPressurePoint fails a claimed
// request and fails it again: one refusal counted as failed and one
// pressure.rejecting_started record; the repeated fail answers unchanged and
// counts nothing.
func TestControllerFailCountsOnceAtTheRequestPressurePoint(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	var records bytes.Buffer
	telemetry := workload.NewTelemetry(workload.TelemetryOptions{Service: ServiceName, Logger: workload.NewLogger(&records, ServiceName, "test", "unit"), SampleRatio: 1})
	t.Cleanup(workload.InstallTelemetry(telemetry))
	f.cloudComputer("sesn_fail_counted", false)
	request := f.ensureRequest("sesn_fail_counted")
	f.claim(request.ID, "worker_a")
	f.requestObject(request.ID, "fail")
	f.requestObject(request.ID, "fail")
	if failed := metricValue(t, f.pods[hubTestPod].metrics, "computerhub_controller_requests_rejected_total", "failed"); failed != 1 {
		t.Fatalf("computerhub_controller_requests_rejected_total{cause=failed} = %v; want 1", failed)
	}
	var started []map[string]any
	decoder := json.NewDecoder(&records)
	for decoder.More() {
		var record map[string]any
		if err := decoder.Decode(&record); err != nil {
			t.Fatal(err)
		}
		if record["event"] == "pressure.rejecting_started" {
			started = append(started, record)
		}
	}
	if len(started) != 1 || started[0]["pressure.point"] != "computerhub_controller_requests" || started[0]["pressure.cause"] != "failed" {
		t.Fatalf("pressure records = %v; want one rejecting_started for failed", started)
	}
}
