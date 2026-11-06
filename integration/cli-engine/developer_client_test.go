package joint

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"
)

// developerClient is an HTTP client in the developer's place: it calls the
// Session routes through the public edge with the workspace API key.
type developerClient struct {
	t       *testing.T
	baseURL string
	apiKey  string
	record  *acceptanceRecord
}

func (d *developerClient) get(ctx context.Context, path string, target any) {
	d.t.Helper()
	status := d.getQuiet(ctx, path, target)
	d.record.command("developer `GET %s` answered %d", path, status)
}

// getQuiet is get without a record line, for reads a test repeats.
func (d *developerClient) getQuiet(ctx context.Context, path string, target any) int {
	d.t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, d.baseURL+path, nil)
	if err != nil {
		d.t.Fatal(err)
	}
	request.Header.Set("X-Api-Key", d.apiKey)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		d.t.Fatalf("GET %s: %v", path, err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		d.t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		d.t.Fatalf("GET %s answered %d %s", path, response.StatusCode, body)
	}
	if err := json.Unmarshal(body, target); err != nil {
		d.t.Fatalf("decode GET %s: %v: %s", path, err, body)
	}
	return response.StatusCode
}

// listSessions is the first page of the workspace's Sessions.
func (d *developerClient) listSessions(ctx context.Context) []json.RawMessage {
	d.t.Helper()
	var page struct {
		Data []json.RawMessage `json:"data"`
	}
	d.get(ctx, "/v1/sessions?beta=true&limit=100", &page)
	return page.Data
}

// post sends body to path and decodes the answer, which must be want, into
// target.
func (d *developerClient) post(ctx context.Context, path string, body any, want int, target any) {
	d.t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		d.t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, d.baseURL+path, bytes.NewReader(data))
	if err != nil {
		d.t.Fatal(err)
	}
	request.Header.Set("X-Api-Key", d.apiKey)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		d.t.Fatalf("POST %s: %v", path, err)
	}
	defer func() { _ = response.Body.Close() }()
	answer, err := io.ReadAll(response.Body)
	if err != nil {
		d.t.Fatal(err)
	}
	d.record.command("developer `POST %s` answered %d", path, response.StatusCode)
	if response.StatusCode != want {
		d.t.Fatalf("POST %s answered %d %s; want %d", path, response.StatusCode, answer, want)
	}
	if err := json.Unmarshal(answer, target); err != nil {
		d.t.Fatalf("decode POST %s: %v: %s", path, err, answer)
	}
}

// cloudSession is a Session with one cloud computer.
type cloudSession struct {
	sessionID, computerID, environmentID string
}

func (d *developerClient) createAgent(ctx context.Context, name string) string {
	d.t.Helper()
	var agent struct {
		ID string `json:"id"`
	}
	d.post(ctx, "/v1/agents?beta=true", map[string]any{"name": name, "model": "anthropic/claude-opus-4-8",
		"tools": []any{map[string]any{"type": "tetral_agent_toolset", "family": "claude"}}}, http.StatusOK, &agent)
	return agent.ID
}

func (d *developerClient) createEnvironment(ctx context.Context, name string) string {
	d.t.Helper()
	var environment struct {
		ID string `json:"id"`
	}
	d.post(ctx, "/v1/environments?beta=true", map[string]any{"name": name}, http.StatusOK, &environment)
	return environment.ID
}

// sessionObject is the part of a Session the scenarios read.
type sessionObject struct {
	ID        string           `json:"id"`
	Computers []computerObject `json:"computers"`
	Drives    []sessionDrive   `json:"drives"`
}

// createSession creates a Session of agentID with the computer items
// computers, as the Session creation request takes them.
func (d *developerClient) createSession(ctx context.Context, agentID string, computers ...map[string]any) sessionObject {
	d.t.Helper()
	return d.createSessionWithDrives(ctx, agentID, nil, computers...)
}

// createSessionWithDrives creates a Session as createSession does that binds
// drives, when there are any.
func (d *developerClient) createSessionWithDrives(ctx context.Context, agentID string, drives []sessionDrive, computers ...map[string]any) sessionObject {
	d.t.Helper()
	body := map[string]any{"agent": agentID, "vault_ids": []string{}, "computers": computers}
	if drives != nil {
		body["drives"] = drives
	}
	var session sessionObject
	d.post(ctx, "/v1/sessions?beta=true", body, http.StatusOK, &session)
	if len(session.Computers) != len(computers) {
		d.t.Fatalf("created Session %s with computers %+v; want %d", session.ID, session.Computers, len(computers))
	}
	return session
}

// unservedCloudComputer is the computer item of a cloud computer of a new
// Environment that no machine serves: the one cloud computer every Session
// lists, in a scenario about its user computers.
func (d *developerClient) unservedCloudComputer(ctx context.Context, name string) map[string]any {
	d.t.Helper()
	return map[string]any{"environment_id": d.createEnvironment(ctx, name), "working_directory": "/workspace"}
}

// createCloudSession creates an agent, an Environment and a Session whose one
// computer is a cloud computer of that Environment with workingDirectory,
// which the machine's CLI creates at preparation.
func (d *developerClient) createCloudSession(ctx context.Context, name, workingDirectory string) cloudSession {
	d.t.Helper()
	environmentID := d.createEnvironment(ctx, name)
	session := d.createSession(ctx, d.createAgent(ctx, name), map[string]any{"environment_id": environmentID, "working_directory": workingDirectory})
	if session.Computers[0].Kind != "cloud" {
		d.t.Fatalf("created Session %s with computers %+v; want one cloud computer", session.ID, session.Computers)
	}
	return cloudSession{sessionID: session.ID, computerID: session.Computers[0].ID, environmentID: environmentID}
}

// computerObject is the part of a Session computer the scenarios read.
type computerObject struct {
	ID                string  `json:"id"`
	Kind              string  `json:"kind"`
	CurrentInstanceID *string `json:"current_instance_id"`
	WorkingDirectory  string  `json:"working_directory"`
	CWD               *string `json:"cwd"`
	AssociationState  string  `json:"association_state"`
	ExecutionState    string  `json:"execution_state"`
	ConnectionID      *string `json:"connection_id"`
	BindingID         *string `json:"binding_id"`
	Repositories      []struct {
		URL  string `json:"url"`
		Path string `json:"path"`
	} `json:"repositories"`
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (d *developerClient) readComputer(ctx context.Context, s cloudSession) computerObject {
	d.t.Helper()
	return d.readSessionComputer(ctx, s.sessionID, s.computerID)
}

func (d *developerClient) readSessionComputer(ctx context.Context, sessionID, computerID string) computerObject {
	d.t.Helper()
	var object computerObject
	d.get(ctx, "/v1/sessions/"+sessionID+"/computers/"+computerID+"?beta=true", &object)
	return object
}

// waitComputer reads computerID of sessionID every half second until done
// accepts it, failing the test after timeout.
func (d *developerClient) waitComputer(ctx context.Context, sessionID, computerID string, timeout time.Duration, what string, done func(computerObject) bool) computerObject {
	d.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var object computerObject
		d.getQuiet(ctx, "/v1/sessions/"+sessionID+"/computers/"+computerID+"?beta=true", &object)
		if done(object) {
			d.record.command("developer `GET /v1/sessions/%s/computers/%s` read %s", sessionID, computerID, what)
			return object
		}
		if time.Now().After(deadline) {
			d.t.Fatalf("computer %s of %s: %s not reached within %s; last read %+v", computerID, sessionID, what, timeout, object)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// sessionError is a session.error event of a Session.
type sessionError struct {
	Type  string `json:"type"`
	Error struct {
		Type        string `json:"type"`
		Message     string `json:"message"`
		ComputerID  string `json:"computer_id"`
		Code        string `json:"code"`
		RetryStatus struct {
			Type string `json:"type"`
		} `json:"retry_status"`
	} `json:"error"`
}

// sessionErrors is every session.error event of the first page of the
// Session's events.
func (d *developerClient) sessionErrors(ctx context.Context, sessionID string) []sessionError {
	d.t.Helper()
	var page struct {
		Data []json.RawMessage `json:"data"`
	}
	d.get(ctx, "/v1/sessions/"+sessionID+"/events?beta=true&limit=100", &page)
	var errs []sessionError
	for _, raw := range page.Data {
		var event sessionError
		if err := json.Unmarshal(raw, &event); err != nil {
			d.t.Fatalf("decode event %s: %v", raw, err)
		}
		if event.Type == "session.error" {
			errs = append(errs, event)
		}
	}
	return errs
}

// archiveSession archives the Session, which closes its computers.
func (d *developerClient) archiveSession(ctx context.Context, sessionID string) {
	d.t.Helper()
	if status, _, answer := d.do(ctx, http.MethodPost, "/v1/sessions/"+sessionID+"/archive?beta=true", nil, ""); status != http.StatusOK {
		d.t.Fatalf("archiving Session %s answered %d %s; want 200", sessionID, status, answer)
	}
}

// removeComputer sends DELETE of the Session's computer and returns the
// answer's status and the computer object it carries, if any.
func (d *developerClient) removeComputer(ctx context.Context, sessionID, computerID string) (int, computerObject) {
	d.t.Helper()
	status, _, answer := d.do(ctx, http.MethodDelete, "/v1/sessions/"+sessionID+"/computers/"+computerID+"?beta=true", nil, "")
	var object computerObject
	if len(answer) > 0 {
		if err := json.Unmarshal(answer, &object); err != nil {
			d.t.Fatalf("decode the removal's answer %s: %v", answer, err)
		}
	}
	return status, object
}

// waitClosed repeats the removal of a detached computer, which is answered
// from its closure, until it answers 204, the computer closed.
func (d *developerClient) waitClosed(ctx context.Context, sessionID, computerID string, timeout time.Duration) {
	d.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		status, object := d.removeComputer(ctx, sessionID, computerID)
		if status == http.StatusNoContent {
			return
		}
		if status != http.StatusAccepted || time.Now().After(deadline) {
			d.t.Fatalf("removal of computer %s answered %d %+v; want 204 within %s", computerID, status, object, timeout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// addComputer adds the computer item to the Session with a fresh
// Idempotency-Key and returns the answer's status and body.
func (d *developerClient) addComputer(ctx context.Context, sessionID string, item map[string]any) (int, string) {
	d.t.Helper()
	data, err := json.Marshal(item)
	if err != nil {
		d.t.Fatal(err)
	}
	status, _, answer := d.do(ctx, http.MethodPost, "/v1/sessions/"+sessionID+"/computers?beta=true", data, "application/json", "Idempotency-Key", randomHex(d.t, 8))
	return status, string(answer)
}
