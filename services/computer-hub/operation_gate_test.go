package computerhub

import (
	"io"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
)

// TestInstanceRoutesRequireTheirOwnOperations proves that the shared
// operation gate admits each instance route only under the operation its
// route entry names: all other operations together are refused before the
// handler runs, and that operation alone passes the gate.
func TestInstanceRoutesRequireTheirOwnOperations(t *testing.T) {
	f := newHubFixture(t, time.Hour)
	for _, route := range []struct {
		method, path string
		operation    auth.Operation
	}{
		{http.MethodPost, "/v1/instances", "instances.create"},
		{http.MethodGet, "/v1/instances", "instances.list"},
		{http.MethodGet, "/v1/instances/stream", "instances.stream"},
		{http.MethodGet, "/v1/instances/inst_absent", "instances.read"},
		{http.MethodDelete, "/v1/instances/inst_absent", "instances.delete"},
		{http.MethodPost, "/v1/instances/inst_absent/credentials", "instance_credentials.create"},
		{http.MethodGet, "/v1/instances/inst_absent/credentials", "instance_credentials.list"},
		{http.MethodDelete, "/v1/instances/inst_absent/credentials/ccred_absent", "instance_credentials.delete"},
	} {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			others := slices.DeleteFunc(auth.RegisteredOperations(), func(operation auth.Operation) bool { return operation == route.operation })
			status, body := f.gateResponse(route.method, route.path, others)
			requireAPIError(t, status, body, http.StatusForbidden, "permission_error", "")
			if status, body := f.gateResponse(route.method, route.path, []auth.Operation{route.operation}); status == http.StatusForbidden {
				t.Fatalf("with only %s: status %d body=%s", route.operation, status, body)
			}
		})
	}
}

// gateResponse sends one request as the fixture's principal holding exactly
// operations. It reads the body of a 403 only, so it never waits on a stream.
func (f *hubFixture) gateResponse(method, path string, operations []auth.Operation) (int, []byte) {
	f.t.Helper()
	principal := f.principal
	principal.Authority.Operations = operations
	token, err := f.signer.Mint(principal, method, path, "req_gate", time.Minute)
	if err != nil {
		f.t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(f.ctx, method, f.url+path, nil)
	if err != nil {
		f.t.Fatal(err)
	}
	request.Header.Set("X-Tetral-Internal-Principal", token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		f.t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusForbidden {
		return response.StatusCode, nil
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		f.t.Fatal(err)
	}
	return response.StatusCode, body
}

// TestPathsOutsideTheRouteGroupsVerifyThePrincipalFirst sends requests to
// paths no route group holds: without a signed principal each answers 401,
// and with a workspace principal 404 not_found_error.
func TestPathsOutsideTheRouteGroupsVerifyThePrincipalFirst(t *testing.T) {
	f := newHubFixture(t, time.Hour)
	for _, path := range []string{"/v1/client", "/v1/other"} {
		request, err := http.NewRequestWithContext(f.ctx, http.MethodGet, f.url+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		requireAPIError(t, response.StatusCode, body, http.StatusUnauthorized, "authentication_error", "")
		status, body := f.do(http.MethodGet, path, nil, "")
		requireAPIError(t, status, body, http.StatusNotFound, "not_found_error", "")
	}
}
