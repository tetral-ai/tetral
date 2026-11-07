package joint

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"slices"
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
	delays  map[string]time.Duration
	events  []eventsRequest
	reports []capturedReport
	// holds keeps an instance's client event requests from going on while
	// its hold is in place.
	holds map[string]*eventsHold
	// rules are the faults in place, in the order added.
	rules []*edgeRule
	// requests is every request the edge answered, in arrival order.
	requests []*edgeRequest
	// streams is what each instance's client event streams carried, in
	// order.
	streams map[string]*lockedBuffer
}

// edgeRule is a fault the edge applies to the requests match accepts, after
// Check allowed them. before runs first and answers the request itself when
// it returns true; it may block, as a held upload does. after, when set,
// changes the backend's answer. lose, when set, cuts the client's connection
// once the backend has answered, so the answer is lost.
type edgeRule struct {
	match  func(*http.Request) bool
	before func(http.ResponseWriter, *http.Request) bool
	after  func(*http.Response) error
	lose   bool
}

// edgeRequest is one request the edge answered: its method and path, the
// status of its answer and when it arrived and ended.
type edgeRequest struct {
	method, path string
	status       int
	at, ended    time.Time
}

// statusRecorder keeps the status a handler wrote.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (w *statusRecorder) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusRecorder) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(p)
}

func (w *statusRecorder) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *statusRecorder) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// addRule puts rule in place and returns the function that removes it.
func (e *edge) addRule(rule *edgeRule) func() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rules = append(e.rules, rule)
	return func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.rules = slices.DeleteFunc(e.rules, func(r *edgeRule) bool { return r == rule })
	}
}

// scope returns a function that puts the edge's rules and delays back as
// they are now, so that what a table test's row puts in place ends with it.
func (e *edge) scope() func() {
	e.mu.Lock()
	defer e.mu.Unlock()
	rules, delays := slices.Clone(e.rules), maps.Clone(e.delays)
	return func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.rules, e.delays = rules, delays
	}
}

// rule is the first rule in place that matches r, or nil.
func (e *edge) rule(r *http.Request) *edgeRule {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, rule := range e.rules {
		if rule.match(r) {
			return rule
		}
	}
	return nil
}

// answered is every request the edge answered whose method is method and
// whose path matches path, in arrival order.
func (e *edge) answered(method string, path *regexp.Regexp) []edgeRequest {
	e.mu.Lock()
	defer e.mu.Unlock()
	var requests []edgeRequest
	for _, request := range e.requests {
		if request.method == method && path.MatchString(request.path) {
			requests = append(requests, *request)
		}
	}
	return requests
}

// streamed is what instanceID's client event streams carried so far.
func (e *edge) streamed(instanceID string) string {
	e.mu.Lock()
	buffer := e.streams[instanceID]
	e.mu.Unlock()
	if buffer == nil {
		return ""
	}
	return buffer.String()
}

// streamCopy copies what a stream's body yields into a buffer as the proxy
// reads it.
type streamCopy struct {
	io.ReadCloser
	into *lockedBuffer
}

func (c streamCopy) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	_, _ = c.into.Write(p[:n])
	return n, err
}

// eventsHold holds one instance's client event requests at the edge until
// released; arrived is closed when the first one arrives.
type eventsHold struct {
	arrived, released chan struct{}
	first             sync.Once
}

// capturedReport keeps actual upstream bytes and credentials only in test memory.
type capturedReport struct {
	body    []byte
	headers http.Header
	status  int
	ack     []byte
	// answered is when the backend's answer reached the edge.
	answered time.Time
}

// backend is one origin of a service, named like its pod.
type backend struct {
	name   string
	origin *url.URL
	down   atomic.Bool
}

// eventsRequest is one GET /v1/client/events the edge forwarded: when it
// arrived, its credential and predecessor query, the backend's status and,
// once the backend's response ended, when.
type eventsRequest struct {
	backend    string
	instance   string
	credential string
	previous   string
	resume     string
	status     int
	at         time.Time
	ended      time.Time
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

// errLostAnswer cuts the client's connection after the backend answered.
var errLostAnswer = errors.New("the edge loses this answer")

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
func startEdge(t *testing.T, listener net.Listener, authGRPCAddress string, services map[string][][2]string) (*edge, string, string) {
	t.Helper()
	connection, err := grpc.NewClient(authGRPCAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	e := &edge{check: authv3.NewAuthorizationClient(connection), services: map[string][]*backend{}, delays: map[string]time.Duration{}, holds: map[string]*eventsHold{}, streams: map[string]*lockedBuffer{}}
	for name, backends := range services {
		for _, named := range backends {
			origin, err := url.Parse(named[1])
			if err != nil {
				t.Fatal(err)
			}
			e.services[name] = append(e.services[name], &backend{name: named[0], origin: origin})
		}
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

// holdEvents holds instanceID's client event requests until release.
func (e *edge) holdEvents(instanceID string) *eventsHold {
	hold := &eventsHold{arrived: make(chan struct{}), released: make(chan struct{})}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.holds[instanceID] = hold
	return hold
}

// releaseEvents lets instanceID's held client event requests go on.
func (e *edge) releaseEvents(instanceID string) {
	e.mu.Lock()
	hold := e.holds[instanceID]
	delete(e.holds, instanceID)
	e.mu.Unlock()
	if hold != nil {
		close(hold.released)
	}
}

// waitArrived waits up to timeout for the first held request.
func (h *eventsHold) waitArrived(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-h.arrived:
	case <-time.After(timeout):
		t.Fatalf("no client event request arrived at the hold within %s", timeout)
	}
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
	if r.URL.Path == "/v1/client/events" {
		e.mu.Lock()
		hold := e.holds[r.URL.Query().Get("instance_id")]
		e.mu.Unlock()
		if hold != nil {
			hold.first.Do(func() { close(hold.arrived) })
			select {
			case <-hold.released:
			case <-r.Context().Done():
				return
			}
		}
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
	answered := &edgeRequest{method: r.Method, path: r.URL.Path, at: time.Now()}
	e.mu.Lock()
	e.requests = append(e.requests, answered)
	e.mu.Unlock()
	recorder := &statusRecorder{ResponseWriter: w}
	w = recorder
	defer func() {
		e.mu.Lock()
		answered.status, answered.ended = recorder.status, time.Now()
		e.mu.Unlock()
	}()
	rule := e.rule(r)
	if rule != nil && rule.before != nil && rule.before(w, r) {
		return
	}
	var captured *capturedReport
	if r.URL.Path == "/v1/client/session-status" {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read report", http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		captured = &capturedReport{body: body, headers: r.Header.Clone()}
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
			if captured != nil {
				body, err := io.ReadAll(response.Body)
				if err != nil {
					return err
				}
				_ = response.Body.Close()
				response.Body = io.NopCloser(bytes.NewReader(body))
				captured.status, captured.ack, captured.answered = response.StatusCode, body, time.Now()
				e.mu.Lock()
				e.reports = append(e.reports, *captured)
				e.mu.Unlock()
			}

			if r.URL.Path == "/v1/client/events" {
				query := r.URL.Query()
				e.mu.Lock()
				index := len(e.events)
				e.events = append(e.events, eventsRequest{backend: target.name, instance: query.Get("instance_id"), credential: query.Get("credential_id"), previous: query.Get("previous_connection_id"), resume: query.Get("resume"), status: response.StatusCode, at: arrived})
				into := e.streams[query.Get("instance_id")]
				if into == nil {
					into = &lockedBuffer{}
					e.streams[query.Get("instance_id")] = into
				}
				e.mu.Unlock()
				response.Body = endRecorder{ReadCloser: streamCopy{ReadCloser: response.Body, into: into}, ended: func() {
					e.mu.Lock()
					defer e.mu.Unlock()
					e.events[index].ended = time.Now()
				}}
			}
			if rule != nil && rule.after != nil {
				if err := rule.after(response); err != nil {
					return err
				}
			}
			if rule != nil && rule.lose {
				return errLostAnswer
			}
			if delay > 0 {
				time.Sleep(delay)
			}
			return nil
		},
		// Client event streams reach the CLI as each event is written.
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			if errors.Is(err, errLostAnswer) {
				panic(http.ErrAbortHandler)
			}
			w.WriteHeader(http.StatusBadGateway)
		},
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
