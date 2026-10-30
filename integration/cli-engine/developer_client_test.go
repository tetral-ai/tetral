package joint

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
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
	d.record.command("developer `GET %s` answered %d", path, response.StatusCode)
	if response.StatusCode != http.StatusOK {
		d.t.Fatalf("GET %s answered %d %s", path, response.StatusCode, body)
	}
	if err := json.Unmarshal(body, target); err != nil {
		d.t.Fatalf("decode GET %s: %v: %s", path, err, body)
	}
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

// createCloudSession creates an agent, an Environment and a Session whose one
// computer is a cloud computer of that Environment.
func (d *developerClient) createCloudSession(ctx context.Context, name string) cloudSession {
	d.t.Helper()
	var agent, environment struct {
		ID string `json:"id"`
	}
	d.post(ctx, "/v1/agents?beta=true", map[string]any{"name": name, "model": "anthropic/claude-opus-4-8",
		"tools": []any{map[string]any{"type": "tetral_agent_toolset", "family": "claude"}}}, http.StatusOK, &agent)
	d.post(ctx, "/v1/environments?beta=true", map[string]any{"name": name}, http.StatusOK, &environment)
	var session struct {
		ID        string           `json:"id"`
		Computers []computerObject `json:"computers"`
	}
	d.post(ctx, "/v1/sessions?beta=true", map[string]any{"agent": agent.ID, "vault_ids": []string{},
		"computers": []any{map[string]any{"environment_id": environment.ID}}}, http.StatusOK, &session)
	if len(session.Computers) != 1 || session.Computers[0].Kind != "cloud" {
		d.t.Fatalf("created Session %s with computers %+v; want one cloud computer", session.ID, session.Computers)
	}
	return cloudSession{sessionID: session.ID, computerID: session.Computers[0].ID, environmentID: environment.ID}
}

// computerObject is the part of a Session computer the scenarios read.
type computerObject struct {
	ID                string  `json:"id"`
	Kind              string  `json:"kind"`
	CurrentInstanceID *string `json:"current_instance_id"`
}

// readComputer reads the Session's computer.
func (d *developerClient) readComputer(ctx context.Context, s cloudSession) computerObject {
	d.t.Helper()
	var object computerObject
	d.get(ctx, "/v1/sessions/"+s.sessionID+"/computers/"+s.computerID+"?beta=true", &object)
	return object
}
