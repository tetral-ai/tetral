package joint

import (
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
