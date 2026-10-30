package joint

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// controllerClient is an HTTP client in a Controller's place: it calls the
// instance routes through the public edge with the workspace API key.
type controllerClient struct {
	t       *testing.T
	baseURL string
	apiKey  string
	record  *acceptanceRecord
}

// apiResponse is one answer of a management route.
type apiResponse struct {
	status int
	body   []byte
}

func (c *controllerClient) do(ctx context.Context, method, path, idempotencyKey string, body any) apiResponse {
	c.t.Helper()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			c.t.Fatal(err)
		}
		reader = bytes.NewReader(data)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		c.t.Fatal(err)
	}
	request.Header.Set("X-Api-Key", c.apiKey)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		c.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		c.t.Fatal(err)
	}
	c.record.command("Controller `%s %s` answered %d", method, path, response.StatusCode)
	return apiResponse{status: response.StatusCode, body: data}
}

func (c *controllerClient) decode(response apiResponse, want int, target any) {
	c.t.Helper()
	if response.status != want {
		c.t.Fatalf("answer %d %s; want %d", response.status, response.body, want)
	}
	if target != nil {
		if err := json.Unmarshal(response.body, target); err != nil {
			c.t.Fatalf("decode %s: %v", response.body, err)
		}
	}
}

// instanceObject is the part of the instance object the scenarios read.
type instanceObject struct {
	ID                  string            `json:"id"`
	Kind                string            `json:"kind"`
	ComputerID          *string           `json:"computer_id"`
	RegistrationState   string            `json:"registration_state"`
	Unused              bool              `json:"unused"`
	Connection          *connectionObject `json:"connection"`
	CLIVersion          *string           `json:"cli_version"`
	CLIUpdateAvailable  bool              `json:"cli_update_available"`
	ComputerCount       int               `json:"computer_count"`
	BindingCount        int               `json:"binding_count"`
	LastCredentialReuse *struct {
		CredentialID string    `json:"credential_id"`
		DetectedAt   time.Time `json:"detected_at"`
	} `json:"last_credential_reuse"`
}

type connectionObject struct {
	ID              string          `json:"id"`
	CredentialID    string          `json:"credential_id"`
	State           string          `json:"state"`
	ReadyAt         *time.Time      `json:"ready_at"`
	LastHeartbeatAt *time.Time      `json:"last_heartbeat_at"`
	LeaseExpiresAt  time.Time       `json:"lease_expires_at"`
	Capabilities    json.RawMessage `json:"capabilities"`
	Error           *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type credentialObject struct {
	ID            string     `json:"id"`
	InstanceID    string     `json:"instance_id"`
	State         string     `json:"state"`
	InUse         bool       `json:"in_use"`
	ExpiresAt     time.Time  `json:"expires_at"`
	RevokedAt     *time.Time `json:"revoked_at"`
	ConnectionKey *string    `json:"connection_key"`
}

// registration is a created user instance with its first credential.
type registration struct {
	instance   instanceObject
	credential credentialObject
}

// createUserInstance registers a user instance named name.
func (c *controllerClient) createUserInstance(ctx context.Context, name string) registration {
	c.t.Helper()
	var created struct {
		Instance   instanceObject   `json:"instance"`
		Credential credentialObject `json:"credential"`
	}
	c.decode(c.do(ctx, http.MethodPost, "/v1/instances", fmt.Sprintf("create-%s-%d", name, time.Now().UnixNano()),
		map[string]any{"name": name, "credential": map[string]any{}}), http.StatusCreated, &created)
	if created.Credential.ConnectionKey == nil {
		c.t.Fatal("the creation response holds no connection key")
	}
	return registration{instance: created.Instance, credential: created.Credential}
}

// issueStreamCredential issues a credential with delivery stream, which
// renews the instance's online connection: 202 with a pending credential and
// no key.
func (c *controllerClient) issueStreamCredential(ctx context.Context, instanceID string) credentialObject {
	c.t.Helper()
	var credential credentialObject
	c.decode(c.do(ctx, http.MethodPost, "/v1/instances/"+instanceID+"/credentials", fmt.Sprintf("renew-%s-%d", instanceID, time.Now().UnixNano()),
		map[string]any{"delivery": "stream"}), http.StatusAccepted, &credential)
	if credential.State != "pending" || credential.ConnectionKey != nil {
		c.t.Fatalf("stream issuance: credential %+v; want pending without a key", credential)
	}
	return credential
}

// issueResponseCredential issues a credential with delivery response.
func (c *controllerClient) issueResponseCredential(ctx context.Context, instanceID string) credentialObject {
	c.t.Helper()
	var credential credentialObject
	c.decode(c.do(ctx, http.MethodPost, "/v1/instances/"+instanceID+"/credentials", fmt.Sprintf("issue-%s-%d", instanceID, time.Now().UnixNano()),
		map[string]any{"delivery": "response"}), http.StatusCreated, &credential)
	return credential
}

func (c *controllerClient) revokeCredential(ctx context.Context, instanceID, credentialID string) {
	c.t.Helper()
	c.decode(c.do(ctx, http.MethodDelete, "/v1/instances/"+instanceID+"/credentials/"+credentialID, "", nil), http.StatusNoContent, nil)
}

func (c *controllerClient) readInstance(ctx context.Context, instanceID string) instanceObject {
	c.t.Helper()
	var object instanceObject
	c.decode(c.do(ctx, http.MethodGet, "/v1/instances/"+instanceID, "", nil), http.StatusOK, &object)
	return object
}

func (c *controllerClient) listCredentials(ctx context.Context, instanceID string) []credentialObject {
	c.t.Helper()
	var page struct {
		Data []credentialObject `json:"data"`
	}
	c.decode(c.do(ctx, http.MethodGet, "/v1/instances/"+instanceID+"/credentials?limit=100", "", nil), http.StatusOK, &page)
	return page.Data
}

// waitInstance reads instanceID until done accepts it or the timeout passes.
func (c *controllerClient) waitInstance(ctx context.Context, instanceID string, timeout time.Duration, what string, done func(instanceObject) bool) instanceObject {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		object := c.readInstance(ctx, instanceID)
		if done(object) {
			return object
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("instance %s: %s not reached within %s; last read %+v", instanceID, what, timeout, object)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// bootstrap is the bootstrap object of a credential the Controller issued.
func bootstrap(serverURL, instanceID string, credential credentialObject) []byte {
	data, _ := json.Marshal(map[string]string{
		"server_url":     serverURL,
		"instance_id":    instanceID,
		"credential_id":  credential.ID,
		"connection_key": *credential.ConnectionKey,
		"expires_at":     credential.ExpiresAt.UTC().Format(time.RFC3339Nano),
	})
	return data
}

// requestObject is a Controller request as the request routes serve it.
type requestObject struct {
	ID            string  `json:"id"`
	Kind          string  `json:"kind"`
	EnvironmentID string  `json:"environment_id"`
	InstanceID    *string `json:"instance_id"`
	State         string  `json:"state"`
	WorkerID      *string `json:"worker_id"`
}

// claimResult is the answer to a claim; bootstrap is the object configure
// --from-stdin reads, kept as received.
type claimResult struct {
	Request          requestObject   `json:"request"`
	Bootstrap        json.RawMessage `json:"bootstrap"`
	WorkingDirectory *string         `json:"working_directory"`
}

// bootstrapFields are the IDs of a claim's bootstrap.
func (r claimResult) bootstrapFields(t *testing.T) (instanceID, credentialID string) {
	t.Helper()
	var fields struct {
		InstanceID   string `json:"instance_id"`
		CredentialID string `json:"credential_id"`
	}
	if err := json.Unmarshal(r.Bootstrap, &fields); err != nil {
		t.Fatalf("decode bootstrap %s: %v", r.Bootstrap, err)
	}
	return fields.InstanceID, fields.CredentialID
}

// listRequests is the first page of environmentID's pending requests.
func (c *controllerClient) listRequests(ctx context.Context, environmentID string) []requestObject {
	c.t.Helper()
	var page struct {
		Data []requestObject `json:"data"`
	}
	c.decode(c.do(ctx, http.MethodGet, "/v1/instance_requests?environment_id="+environmentID+"&state=pending&limit=100", "", nil), http.StatusOK, &page)
	return page.Data
}

func (c *controllerClient) claimRequest(ctx context.Context, requestID, workerID string) claimResult {
	c.t.Helper()
	var result claimResult
	c.decode(c.do(ctx, http.MethodPost, "/v1/instance_requests/"+requestID+"/claim", "", map[string]string{"worker_id": workerID}), http.StatusOK, &result)
	return result
}

func (c *controllerClient) failRequest(ctx context.Context, requestID string) requestObject {
	c.t.Helper()
	var object requestObject
	c.decode(c.do(ctx, http.MethodPost, "/v1/instance_requests/"+requestID+"/fail", "", nil), http.StatusOK, &object)
	return object
}

// requestEvent is one event of the request stream.
type requestEvent struct {
	name    string
	request requestObject
}

// openRequestStream subscribes to environmentID's request stream through the
// edge and returns its events as they arrive.
func (c *controllerClient) openRequestStream(ctx context.Context, environmentID string) <-chan requestEvent {
	c.t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/instance_requests/stream?environment_id="+environmentID, nil)
	if err != nil {
		c.t.Fatal(err)
	}
	request.Header.Set("X-Api-Key", c.apiKey)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		c.t.Fatalf("open the request stream: %v", err)
	}
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream" {
		_ = response.Body.Close()
		c.t.Fatalf("request stream answered %d %s", response.StatusCode, response.Header.Get("Content-Type"))
	}
	c.t.Cleanup(func() { _ = response.Body.Close() })
	c.record.command("Controller `GET /v1/instance_requests/stream?environment_id=%s` opened", environmentID)
	events := make(chan requestEvent, 16)
	go func() {
		defer close(events)
		scanner := bufio.NewScanner(response.Body)
		var name string
		for scanner.Scan() {
			line := scanner.Text()
			switch {
			case strings.HasPrefix(line, "event: "):
				name = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				var object requestObject
				if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &object) == nil {
					events <- requestEvent{name: name, request: object}
				}
			}
		}
	}()
	return events
}
