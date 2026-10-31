package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/session"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// sessionStatusFixture drives one Session through Bridge as the Runtime does
// and serves its api computer and archive routes from the same database.
type sessionStatusFixture struct {
	t         *testing.T
	admin     *sql.DB
	store     *PostgreSQLBridgeAPIStore
	router    http.Handler
	now       time.Time
	sessionID string
	bindingID string
	podUID    string
}

func newSessionStatusFixture(t *testing.T, sessionID, mainID string) *sessionStatusFixture {
	t.Helper()
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	f := &sessionStatusFixture{
		t: t, admin: admin, now: time.Date(2026, 10, 30, 9, 0, 0, 0, time.UTC),
		sessionID: sessionID, bindingID: "bind_" + sessionID, podUID: "pod_" + sessionID,
	}
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, f.bindingID, 1, f.podUID)
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO computers
		(workspace_id,session_id,id,kind,environment_id,working_directory,networking,drive_exclude,association_state,execution_state)
		VALUES ('default',$1,$2,'cloud',$3,'/workspace','{"type":"unrestricted"}','[]','attached','waiting')`,
		sessionID, "comp_"+sessionID, "env_"+sessionID); err != nil {
		t.Fatalf("seed cloud computer: %v", err)
	}
	client := dbconnect.NewClientForTesting(runtimeDB)
	f.store = NewPostgreSQLBridgeAPIStore(client)
	f.store.Clock = func() time.Time { return f.now }
	service := session.NewService(nil, nil, nil, nil,
		session.NewPostgreSQLSessionStore(client, session.WithPageTokenSecret([]byte("session-status-secret-123456789"))), nil)
	authenticator := auth.AuthenticatorFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.IndependentKeyPrincipal(workspace.Workspace{ID: workspace.DefaultID}, "ak_session_status"), nil //nolint:gosec // G101: synthetic test API key id
	})
	f.router = httpapi.NewRouter(httpapi.NewSessionHandler(service), "", httpapi.WithAuthenticator(authenticator))
	return f
}

func (f *sessionStatusFixture) scope(threadID string) *bridgev1.RuntimeScope {
	return sessionfixture.BridgeAPIScope(f.sessionID, threadID, f.bindingID, 1, f.podUID)
}

// start writes the thread's running event and returns its durable turn.
func (f *sessionStatusFixture) start(threadID, writeID string) string {
	f.t.Helper()
	f.now = f.now.Add(time.Second)
	response, err := f.store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: f.scope(threadID), RuntimeWriteId: writeID, EventType: "session.status_running", PayloadJson: `{"type":"session.status_running"}`,
	})
	if err != nil || response.GetCommitted() == nil {
		f.t.Fatalf("running WriteEvent %s = %#v/%v; want committed", writeID, response, err)
	}
	return response.GetCommitted().GetEventId()
}

func (f *sessionStatusFixture) idle(threadID, turnID, stopReasonJSON string) {
	f.t.Helper()
	f.now = f.now.Add(time.Second)
	response, err := finishIdleWithStagedCaptureForTest(f.t, f.admin, f.store, &bridgev1.FinishIdleRequest{
		Scope: f.scope(threadID), DurableTurnId: turnID, StopReasonJson: stopReasonJSON,
	})
	if err != nil || response.GetCommitted() == nil {
		f.t.Fatalf("FinishIdle %s = %#v/%v; want committed", turnID, response, err)
	}
}

type sessionStatusEvent struct {
	Type       string
	Thread     string
	StopReason json.RawMessage
}

// primary lists the running and idle events of the Session's primary stream
// in order.
func (f *sessionStatusFixture) primary() []sessionStatusEvent {
	f.t.Helper()
	rows, err := f.admin.QueryContext(context.Background(), `SELECT type,COALESCE(session_thread_id,''),payload_json FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND session_visible
		AND type IN ('session.status_running','session.status_idle','session.thread_status_running','session.thread_status_idle')
		ORDER BY created_at,session_thread_id NULLS LAST,sequence`, f.sessionID)
	if err != nil {
		f.t.Fatalf("read primary stream: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var events []sessionStatusEvent
	for rows.Next() {
		var event sessionStatusEvent
		var payloadJSON string
		if err := rows.Scan(&event.Type, &event.Thread, &payloadJSON); err != nil {
			f.t.Fatal(err)
		}
		var payload struct {
			StopReason json.RawMessage `json:"stop_reason"`
		}
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			f.t.Fatal(err)
		}
		event.StopReason = payload.StopReason
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		f.t.Fatal(err)
	}
	return events
}

func (f *sessionStatusFixture) primaryTypes() []string {
	var types []string
	for _, event := range f.primary() {
		types = append(types, event.Type)
	}
	return types
}

// threadOnlyStatusEvents counts the thread's status events kept off the
// primary stream.
func (f *sessionStatusFixture) threadOnlyStatusEvents(threadID string) int {
	f.t.Helper()
	var count int
	if err := f.admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND NOT session_visible
		AND type IN ('session.status_running','session.status_idle')`, f.sessionID, threadID).Scan(&count); err != nil {
		f.t.Fatal(err)
	}
	return count
}

func (f *sessionStatusFixture) runtimeStatus() (string, sql.NullTime, sql.NullTime) {
	f.t.Helper()
	var status string
	var idleSince, cleanupAfter sql.NullTime
	if err := f.admin.QueryRowContext(context.Background(), `SELECT status,idle_since,cleanup_after FROM session_runtime_status
		WHERE workspace_id='default' AND session_id=$1`, f.sessionID).Scan(&status, &idleSince, &cleanupAfter); err != nil {
		f.t.Fatal(err)
	}
	return status, idleSince, cleanupAfter
}

func (f *sessionStatusFixture) post(path, key, body string) (int, string) {
	f.t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, path+"?beta=true", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("x-api-key", "sk_session_status")
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	f.router.ServeHTTP(recorder, request)
	var response struct {
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	return recorder.Code, response.Error.Type
}

// The Session runs while any thread runs: a main thread that ends its turn
// while a sub-agent runs leaves it running, and the last thread to settle
// writes the one session.status_idle, joining the waiting calls of
// requires_action idles. Rules out a Session that goes idle while a child
// runs, a Session event written per thread, and a waiting call missing from
// the Session's idle.
func TestSessionStatusFollowsEveryThread(t *testing.T) {
	const (
		sessionID = "sesn_status_threads"
		mainID    = "sthr_status_threads_main"
		childID   = "sthr_status_threads_child"
	)
	f := newSessionStatusFixture(t, sessionID, mainID)
	sessionfixture.SeedBridgeAPIChildThread(t, f.admin, "default", sessionID, mainID, childID)

	mainTurn := f.start(mainID, "main-running")
	childTurn := f.start(childID, "child-running")
	if got, want := f.primaryTypes(), []string{"session.status_running", "session.thread_status_running"}; !slices.Equal(got, want) {
		t.Fatalf("primary after both starts = %q; want %q", got, want)
	}

	f.idle(mainID, mainTurn, `{"type":"retries_exhausted"}`)
	if status, _, _ := f.runtimeStatus(); status != "running" || slices.Contains(f.primaryTypes(), "session.status_idle") {
		t.Fatalf("after the main idle with the child running: status %s, primary %q; want running and no session.status_idle", status, f.primaryTypes())
	}
	if code, errorType := f.post("/v1/sessions/"+sessionID+"/computers", "add-while-child-runs", `{"environment_id":"env_`+sessionID+`"}`); code != http.StatusConflict || errorType != "invalid_request_error" {
		t.Fatalf("computer addition while the child runs = %d %s; want 409 invalid_request_error", code, errorType)
	}

	f.idle(childID, childTurn, `{"type":"requires_action","event_ids":["sevt_child_waits_for_client"]}`)
	idleAt := f.now
	events := f.primary()
	if got := f.primaryTypes(); !slices.Equal(got, []string{"session.status_running", "session.thread_status_running", "session.thread_status_idle", "session.status_idle"}) {
		t.Fatalf("primary after the child idle = %q", got)
	}
	var stop struct {
		Type     string   `json:"type"`
		EventIDs []string `json:"event_ids"`
	}
	if err := json.Unmarshal(events[3].StopReason, &stop); err != nil || events[3].Thread != "" ||
		stop.Type != "requires_action" || !slices.Equal(stop.EventIDs, []string{"sevt_child_waits_for_client"}) {
		t.Fatalf("Session idle = %+v (%s); want a Session event with requires_action listing the child's call", events[3], events[3].StopReason)
	}
	status, idleSince, cleanupAfter := f.runtimeStatus()
	if status != "idle" || !idleSince.Valid || !idleSince.Time.Equal(idleAt) || !cleanupAfter.Valid || !cleanupAfter.Time.Equal(idleAt.Add(runtimecontrol.IdleCleanupDelay)) {
		t.Fatalf("runtime status = %s idle_since %v cleanup_after %v; want idle at %v with cleanup after the idle delay", status, idleSince, cleanupAfter, idleAt)
	}

	childTurn = f.start(childID, "child-running-again")
	f.idle(childID, childTurn, `{"type":"end_turn"}`)
	events = f.primary()
	var sessionEvents []sessionStatusEvent
	for _, event := range events {
		if event.Thread == "" {
			sessionEvents = append(sessionEvents, event)
		}
	}
	if len(sessionEvents) != 4 || sessionEvents[2].Type != "session.status_running" || sessionEvents[3].Type != "session.status_idle" ||
		string(sessionEvents[3].StopReason) != `{"type":"retries_exhausted"}` {
		t.Fatalf("Session events = %+v; want a second running then an idle with the main thread's retries_exhausted", sessionEvents)
	}
	if got := f.threadOnlyStatusEvents(mainID); got != 2 {
		t.Fatalf("main thread status events off the primary stream = %d; want 2", got)
	}
}

// A Session with only its main thread shows one session.status_running and
// one session.status_idle per turn on its primary stream, the main thread's
// own two events per turn only on its thread stream; while the main thread is
// rescheduled the Session is not archived. Rules out a Session event written
// per thread event and an archive while a thread reschedules.
func TestSessionStatusOfAMainThreadOnly(t *testing.T) {
	const (
		sessionID = "sesn_status_main_only"
		mainID    = "sthr_status_main_only"
	)
	f := newSessionStatusFixture(t, sessionID, mainID)
	f.store.RuntimeBindingTokenHMACKey = []byte("session-status-test-signing-key")

	turn := f.start(mainID, "turn-one")
	f.idle(mainID, turn, `{"type":"end_turn"}`)

	turn = f.start(mainID, "turn-two")
	scope := f.scope(mainID)
	seedBridgeAPIRequestStart(t, f.store, scope, "turn-two-request", "mreq_status_main_only", runtimecontrol.RequestKindAgentProviderRequest, 0)
	ended, err := f.store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "turn-two-request-end", ModelRequestId: "mreq_status_main_only",
		FinishReason: "error", UsageJson: `{}`, IsError: true, ErrorKind: "gateway_stream_error",
		ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "rescheduled"},
		Reschedule:               &bridgev1.RequestEndReschedule{Attempt: 1, Deadline: f.now.Add(time.Minute).Format(time.RFC3339Nano), BackoffMs: 1_000},
	})
	if err != nil || ended.GetCommitted().GetRescheduled() == nil {
		t.Fatalf("rescheduled WriteRequestEnd = %#v/%v; want rescheduled", ended, err)
	}
	if code, errorType := f.post("/v1/sessions/"+sessionID+"/archive", "", ""); code != http.StatusConflict || errorType != "invalid_request_error" {
		t.Fatalf("archive while the main thread reschedules = %d %s; want 409 invalid_request_error", code, errorType)
	}
	var lifecycle, association string
	if err := f.admin.QueryRowContext(context.Background(), `SELECT s.lifecycle_state,c.association_state FROM sessions s
		JOIN computers c ON c.workspace_id=s.workspace_id AND c.session_id=s.id WHERE s.workspace_id='default' AND s.id=$1`, sessionID).Scan(&lifecycle, &association); err != nil {
		t.Fatal(err)
	}
	if lifecycle != "active" || association != "attached" {
		t.Fatalf("after the refused archive: lifecycle %s, computer %s; want active and attached", lifecycle, association)
	}
	f.idle(mainID, turn, `{"type":"end_turn"}`)

	want := []string{"session.status_running", "session.status_idle", "session.status_running", "session.status_idle"}
	if got := f.primaryTypes(); !slices.Equal(got, want) {
		t.Fatalf("primary = %q; want %q", got, want)
	}
	for _, event := range f.primary() {
		if event.Thread != "" {
			t.Fatalf("primary holds the main thread's own %s", event.Type)
		}
	}
	if got := f.threadOnlyStatusEvents(mainID); got != 4 {
		t.Fatalf("main thread status events on its thread stream only = %d; want 4", got)
	}
}
