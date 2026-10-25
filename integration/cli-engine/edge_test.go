package joint

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	authv3 "github.com/envoyproxy/go-control-plane/envoy/service/auth/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
)

// edge stands in for the deployment's public edge. Every request but the
// token exchange first goes to auth's Authorization/Check over gRPC: a
// denial answers with Check's status, headers and body, and an allowed
// request goes on, with the signed principal Check returned, to the service
// that edgeRoute names for its path, round robin over that service's
// backends that are not down.
type edge struct {
	check    authv3.AuthorizationClient
	services map[string][]*backend
	sequence atomic.Int64
	next     atomic.Uint64

	mu sync.Mutex
	// delays holds the answer of a request path that long after its backend
	// answered.
	delays map[string]time.Duration
	events []eventsRequest
}

// backend is one origin of a service, named like its pod.
type backend struct {
	name   string
	origin *url.URL
	down   atomic.Bool
}

// eventsRequest is one GET /v1/client/events the edge forwarded: when it
// arrived, its predecessor query, the backend's status and, once the
// backend's response ended, when.
type eventsRequest struct {
	backend  string
	instance string
	previous string
	resume   string
	status   int
	at       time.Time
	ended    time.Time
}

// endRecorder calls ended when the proxy closes the response body it copied.
type endRecorder struct {
	io.ReadCloser
	ended func()
}

func (r endRecorder) Close() error {
	r.ended()
	return r.ReadCloser.Close()
}

var (
	sessionEventStream = regexp.MustCompile(`^/v1/sessions/[^/]+/events/stream$`)
	threadEventStream  = regexp.MustCompile(`^/v1/sessions/[^/]+/threads/[^/]+/stream$`)
)

// edgeRoute is the deployment's edge route rules: the service a request
// goes to, and whether Check runs first. ok is false for a path no rule
// matches.
func edgeRoute(method, path string) (service string, checked, ok bool) {
	under := func(prefix string) bool { return path == prefix || strings.HasPrefix(path, prefix+"/") }
	switch {
	case method == http.MethodPost && path == "/v1/oauth/token":
		return "auth", false, true
	case sessionEventStream.MatchString(path), threadEventStream.MatchString(path):
		return "event-stream", true, true
	case under("/v1/api_keys"):
		return "auth", true, true
	case under("/v1/client"), under("/v1/instance_requests"), under("/v1/instances"):
		return "computer-hub", true, true
	case under("/v1"):
		return "api", true, true
	}
	return "", false, false
}

// startEdge serves the edge on a loopback port until t ends and returns it
// with its origin and port. services maps a service name to its backends'
// names and HTTP origins; a route to a service the test did not start, or
// whose backends are all down, answers 502.
func startEdge(t *testing.T, authGRPCAddress string, services map[string][][2]string) (*edge, string, string) {
	t.Helper()
	connection, err := grpc.NewClient(authGRPCAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	e := &edge{check: authv3.NewAuthorizationClient(connection), services: map[string][]*backend{}, delays: map[string]time.Duration{}}
	for name, backends := range services {
		for _, named := range backends {
			origin, err := url.Parse(named[1])
			if err != nil {
				t.Fatal(err)
			}
			e.services[name] = append(e.services[name], &backend{name: named[0], origin: origin})
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: e, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Close()
		_ = connection.Close()
	})
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	return e, "http://127.0.0.1:" + port, port
}

// pick returns the next backend of service that is not down.
func (e *edge) pick(service string) *backend {
	backends := e.services[service]
	for range backends {
		candidate := backends[e.next.Add(1)%uint64(len(backends))]
		if !candidate.down.Load() {
			return candidate
		}
	}
	return nil
}

// setDown takes the backend named name out of rotation, as an endpoint
// controller does for a pod that died, or with down false puts it back.
func (e *edge) setDown(name string, down bool) {
	for _, backends := range e.services {
		for _, candidate := range backends {
			if candidate.name == name {
				candidate.down.Store(down)
			}
		}
	}
}

// delay holds every answer to path that long after its backend answered.
func (e *edge) delay(path string, d time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.delays[path] = d
}

// eventsRequests returns instanceID's client event requests forwarded so
// far, in arrival order.
func (e *edge) eventsRequests(instanceID string) []eventsRequest {
	e.mu.Lock()
	defer e.mu.Unlock()
	var requests []eventsRequest
	for _, request := range e.events {
		if request.instance == instanceID {
			requests = append(requests, request)
		}
	}
	return requests
}

// waitEvents waits up to timeout for instanceID's n-th client event request
// (n counted from 1) to reach the state done accepts, and returns it.
func (e *edge) waitEvents(t *testing.T, instanceID string, n int, timeout time.Duration, what string, done func(eventsRequest) bool) eventsRequest {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		requests := e.eventsRequests(instanceID)
		if len(requests) >= n && done(requests[n-1]) {
			return requests[n-1]
		}
		if time.Now().After(deadline) {
			t.Fatalf("instance %s: %s not seen at the edge within %s; its event requests %+v", instanceID, what, timeout, requests)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (e *edge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	service, checked, ok := edgeRoute(r.Method, r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	target := e.pick(service)
	if target == nil {
		http.Error(w, service+" does not run in this test", http.StatusBadGateway)
		return
	}
	requestID := fmt.Sprintf("req_edge_%d", e.sequence.Add(1))
	var (
		remove []string
		set    = map[string]string{"X-Request-Id": requestID}
	)
	if checked {
		response, err := e.authorize(r, requestID)
		if err != nil || response.GetStatus().GetCode() != int32(codes.OK) && response.GetDeniedResponse() == nil {
			// The deployment's edge answers a failed Check with 503.
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if denied := response.GetDeniedResponse(); denied != nil {
			for _, header := range denied.GetHeaders() {
				w.Header().Add(header.GetHeader().GetKey(), header.GetHeader().GetValue())
			}
			w.WriteHeader(int(denied.GetStatus().GetCode()))
			_, _ = io.WriteString(w, denied.GetBody()) //nolint:gosec // G705: auth's typed JSON denial with its own Content-Type, never HTML.
			return
		}
		allowed := response.GetOkResponse()
		remove = allowed.GetHeadersToRemove()
		for _, header := range allowed.GetHeaders() {
			set[header.GetHeader().GetKey()] = header.GetHeader().GetValue()
		}
	} else {
		remove = []string{"Authorization", "X-Api-Key"}
	}
	arrived := time.Now()
	e.mu.Lock()
	delay := e.delays[r.URL.Path]
	e.mu.Unlock()
	proxy := &httputil.ReverseProxy{
		Rewrite: func(request *httputil.ProxyRequest) {
			request.SetURL(target.origin)
			request.SetXForwarded()
			for _, name := range remove {
				request.Out.Header.Del(name)
			}
			for name, value := range set {
				request.Out.Header.Set(name, value)
			}
		},
		ModifyResponse: func(response *http.Response) error {
			if r.URL.Path == "/v1/client/events" {
				query := r.URL.Query()
				e.mu.Lock()
				index := len(e.events)
				e.events = append(e.events, eventsRequest{backend: target.name, instance: query.Get("instance_id"), previous: query.Get("previous_connection_id"), resume: query.Get("resume"), status: response.StatusCode, at: arrived})
				e.mu.Unlock()
				response.Body = endRecorder{ReadCloser: response.Body, ended: func() {
					e.mu.Lock()
					defer e.mu.Unlock()
					e.events[index].ended = time.Now()
				}}
			}
			if delay > 0 {
				time.Sleep(delay)
			}
			return nil
		},
		// Client event streams reach the CLI as each event is written.
		FlushInterval: -1,
	}
	proxy.ServeHTTP(w, r)
}

// authorize sends Check the request's method, path and raw headers, with
// the edge's own request ID and forwarded address.
func (e *edge) authorize(r *http.Request, requestID string) (*authv3.CheckResponse, error) {
	headers := &corev3.HeaderMap{}
	for name, values := range r.Header {
		switch strings.ToLower(name) {
		case "x-request-id", "x-forwarded-for":
			continue
		}
		for _, value := range values {
			headers.Headers = append(headers.Headers, &corev3.HeaderValue{Key: strings.ToLower(name), RawValue: []byte(value)})
		}
	}
	headers.Headers = append(headers.Headers,
		&corev3.HeaderValue{Key: "x-request-id", RawValue: []byte(requestID)},
		&corev3.HeaderValue{Key: "x-forwarded-for", RawValue: []byte("127.0.0.1")})
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	return e.check.Check(ctx, &authv3.CheckRequest{Attributes: &authv3.AttributeContext{Request: &authv3.AttributeContext_Request{
		Http: &authv3.AttributeContext_HttpRequest{Method: r.Method, Path: r.URL.RequestURI(), HeaderMap: headers},
	}}})
}
