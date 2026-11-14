package enginetest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/santhosh-tekuri/jsonschema/v6"

	engineapi "github.com/tetral-ai/tetral/api"
)

const apiFile = "file:///api/" + engineapi.File

// apiSpec is Engine's OpenAPI file, read once.
type apiSpec struct {
	operations []*apiOperation
	compiler   *jsonschema.Compiler
	mu         sync.Mutex
	schemas    map[string]*jsonschema.Schema
	// undeclared holds the statuses of a /v1 path the file does not
	// declare, edge those of the edge's own answers.
	undeclared, edge []string
	// edgeTypes is the error type of the edge's answer of each status.
	edgeTypes map[string]string
	// unsupported is the paths of the SDK surfaces Engine does not serve.
	unsupported []*regexp.Regexp
}

type apiOperation struct {
	id, method, path string
	segments         []string
	// pointer is the operation's JSON pointer in the file.
	pointer    string
	parameters []apiParameter
	closed     bool
	body       map[string]bool
	required   bool
	responses  map[string]apiResponse
}

type apiParameter struct {
	name, in string
	required bool
	schema   bool
	// array marks a parameter whose value is every value sent under its
	// name.
	array bool
}

type apiResponse struct {
	// content maps a media type to whether it has a schema; events is a
	// stream's event names.
	content    map[string]bool
	events     map[string]bool
	headers    []string
	errorTypes []string
}

var loadAPI = sync.OnceValues(func() (*apiSpec, error) {
	data, err := engineapi.Files.ReadFile(engineapi.File)
	if err != nil {
		return nil, err
	}
	document, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	var file struct {
		Undeclared struct {
			Statuses    []string `json:"statuses"`
			Unsupported []string `json:"unsupported"`
		} `json:"x-undeclared-paths"`
		Edge struct {
			Statuses []string          `json:"statuses"`
			Types    map[string]string `json:"types"`
		} `json:"x-edge-answers"`
		Paths map[string]map[string]struct {
			OperationID string `json:"operationId"`
			Closed      bool   `json:"x-closed-query"`
			Parameters  []struct {
				Name     string `json:"name"`
				In       string `json:"in"`
				Required bool   `json:"required"`
				Schema   *struct {
					Type string `json:"type"`
				} `json:"schema"`
			} `json:"parameters"`
			RequestBody struct {
				Required bool                       `json:"required"`
				Content  map[string]json.RawMessage `json:"content"`
			} `json:"requestBody"`
			Responses map[string]struct {
				Headers map[string]json.RawMessage `json:"headers"`
				Content map[string]struct {
					Schema json.RawMessage            `json:"schema"`
					Events map[string]json.RawMessage `json:"x-events"`
				} `json:"content"`
				ErrorTypes []string `json:"x-error-types"`
			} `json:"responses"`
		} `json:"paths"`
	}
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, err
	}
	spec := &apiSpec{compiler: jsonschema.NewCompiler(), schemas: map[string]*jsonschema.Schema{}, undeclared: file.Undeclared.Statuses, edge: file.Edge.Statuses, edgeTypes: file.Edge.Types}
	for _, pattern := range file.Undeclared.Unsupported {
		spec.unsupported = append(spec.unsupported, regexp.MustCompile("^"+strings.ReplaceAll(regexp.QuoteMeta(pattern), `\*`, ".*")+"$"))
	}
	spec.compiler.DefaultDraft(jsonschema.Draft2020)
	spec.compiler.UseRegexpEngine(compileECMAScript)
	spec.compiler.AssertFormat()
	if err := spec.compiler.AddResource(apiFile, document); err != nil {
		return nil, err
	}
	for path, methods := range file.Paths {
		for method, declared := range methods {
			operation := &apiOperation{id: declared.OperationID, method: strings.ToUpper(method), path: path, segments: strings.Split(path, "/"),
				pointer: "/paths/" + escapePointer(path) + "/" + method, closed: declared.Closed, body: map[string]bool{},
				required: declared.RequestBody.Required, responses: map[string]apiResponse{}}
			for _, parameter := range declared.Parameters {
				operation.parameters = append(operation.parameters, apiParameter{name: parameter.Name, in: parameter.In, required: parameter.Required,
					schema: parameter.Schema != nil, array: parameter.Schema != nil && parameter.Schema.Type == "array"})
			}
			for media := range declared.RequestBody.Content {
				operation.body[media] = true
			}
			for status, response := range declared.Responses {
				answer := apiResponse{content: map[string]bool{}, errorTypes: response.ErrorTypes}
				for media, content := range response.Content {
					answer.content[media] = content.Schema != nil
					if content.Events != nil {
						answer.events = map[string]bool{}
						for name := range content.Events {
							answer.events[name] = true
						}
					}
				}
				for name := range response.Headers {
					answer.headers = append(answer.headers, name)
				}
				operation.responses[status] = answer
			}
			spec.operations = append(spec.operations, operation)
		}
	}
	return spec, nil
})

func escapePointer(token string) string {
	return strings.ReplaceAll(strings.ReplaceAll(token, "~", "~0"), "/", "~1")
}

// operation returns the operation of method and path, with its path
// parameters; a literal segment wins over a parameter.
func (s *apiSpec) operation(method, path string) (*apiOperation, map[string]string) {
	segments := strings.Split(path, "/")
	var found *apiOperation
	var values map[string]string
	literals := -1
	for _, operation := range s.operations {
		if operation.method != method || len(operation.segments) != len(segments) {
			continue
		}
		matched, count := map[string]string{}, 0
		for i, segment := range operation.segments {
			switch {
			case strings.HasPrefix(segment, "{") && segments[i] != "":
				matched[strings.Trim(segment, "{}")] = segments[i]
			case segment == segments[i]:
				count++
			default:
				matched = nil
			}
			if matched == nil {
				break
			}
		}
		if matched != nil && count > literals {
			found, values, literals = operation, matched, count
		}
	}
	return found, values
}

// undeclaredStatus is the status the file's rule gives a request with valid
// credentials to a path it does not declare for the request's method.
func (s *apiSpec) undeclaredStatus(method, path string) int {
	for _, unsupported := range s.unsupported {
		if unsupported.MatchString(path) {
			return http.StatusBadRequest
		}
	}
	for _, operation := range s.operations {
		if operation.method != method {
			if other, _ := s.operation(operation.method, path); other != nil {
				return http.StatusMethodNotAllowed
			}
		}
	}
	return http.StatusNotFound
}

// validate holds value to the schema at pointer of the file.
func (s *apiSpec) validate(pointer string, value any) error {
	s.mu.Lock()
	schema, found := s.schemas[pointer]
	if !found {
		compiled, err := s.compiler.Compile(apiFile + "#" + pointer)
		if err != nil {
			s.mu.Unlock()
			return fmt.Errorf("the file's schema at %s: %w", pointer, err)
		}
		schema, s.schemas[pointer] = compiled, compiled
	}
	s.mu.Unlock()
	if err := schema.Validate(value); err != nil {
		text := err.Error()
		if _, causes, cut := strings.Cut(text, "\n"); cut {
			text = strings.TrimSpace(causes)
		}
		return errors.New(text)
	}
	return nil
}

// Developer is a test's client of Engine's public API, a developer's or a
// Controller's. It sends exactly the request a test gives it, with no retry
// and no repair, and holds the request, the answer with its headers and
// every stream event to Engine's OpenAPI file: a request outside it fails
// the test as the test's defect, an answer or event outside it as Engine's.
// A request made invalid on purpose, or sent to a path the file does not
// declare, is marked, through Invalid or InvalidRequest: only its answer is
// checked, and it must be a refusal.
//
// A test sends through Do and Stream, or keeps the requests it builds and
// sends them through Handler (a router called in process) or Transport (an
// HTTP client's), or writes them to a socket itself and holds them with
// Exchange; all check alike.
type Developer struct {
	BaseURL string
	// HTTP sends the requests of Do and Stream.
	HTTP *http.Client
	// Authorize adds the credentials of a request of Do and Stream.
	Authorize func(*http.Request) error

	t       T
	invalid bool
}

// NewDeveloper returns a client that reports to t.
func NewDeveloper(t T) *Developer { return &Developer{t: t, HTTP: http.DefaultClient} }

// Invalid returns the client for one request of Do or Stream that is
// outside the file on purpose.
func (d *Developer) Invalid() *Developer {
	marked := *d
	marked.invalid = true
	return &marked
}

type invalidRequest struct{}

// InvalidRequest marks a request sent through Handler or Transport as
// outside the file on purpose.
func InvalidRequest(request *http.Request) *http.Request {
	return request.WithContext(InvalidContext(request.Context()))
}

// InvalidContext returns ctx with every request built on it marked as
// InvalidRequest marks one.
func InvalidContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, invalidRequest{}, true)
}

// InvalidRequests returns router with every request sent through it marked
// as InvalidRequest marks one.
func InvalidRequests(router http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		router.ServeHTTP(w, InvalidRequest(request))
	})
}

type streamedRequest struct{}

// StreamedRequest marks a request sent through Transport whose body is
// produced while it is sent: the body is passed on as it comes, and is the
// one part of the request not held to the file.
func StreamedRequest(request *http.Request) *http.Request {
	return request.WithContext(context.WithValue(request.Context(), streamedRequest{}, true))
}

// InProcess returns an HTTP client that serves each request from handler
// without a connection; the whole answer is written before it returns.
func InProcess(handler http.Handler) *http.Client {
	return &http.Client{Transport: roundTrip(func(request *http.Request) (*http.Response, error) {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		return recorder.Result(), nil
	})}
}

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func (d *Developer) report(format string, args ...any) {
	d.t.Helper()
	d.t.Errorf("enginetest: "+format, args...)
}

// Do sends one request and returns its whole answer.
func (d *Developer) Do(ctx context.Context, method, target string, header http.Header, body []byte) (Response, error) {
	d.t.Helper()
	response, err := d.send(ctx, method, target, header, body)
	if err != nil {
		return Response{}, err
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(response.Body)
	return Response{Status: response.StatusCode, Header: response.Header, Body: data}, err
}

// DeveloperEvent is one event of a stream: its name and its data.
type DeveloperEvent struct {
	Name string
	Data []byte
}

// DeveloperStream is an open event stream of the public API.
type DeveloperStream struct {
	body     io.ReadCloser
	frames   frames
	events   []DeveloperEvent
	comments atomic.Int64
}

// Stream sends one request whose success answer is an event stream. A 200
// answer returns the open stream; any other answer returns its Response.
func (d *Developer) Stream(ctx context.Context, method, target string, header http.Header) (*DeveloperStream, Response, error) {
	d.t.Helper()
	response, err := d.send(ctx, method, target, header, nil)
	if err != nil {
		return nil, Response{}, err
	}
	if response.StatusCode != http.StatusOK {
		defer func() { _ = response.Body.Close() }()
		data, err := io.ReadAll(response.Body)
		return nil, Response{Status: response.StatusCode, Header: response.Header, Body: data}, err
	}
	return &DeveloperStream{body: response.Body}, Response{Status: response.StatusCode, Header: response.Header}, nil
}

// Close closes the stream.
func (s *DeveloperStream) Close() error { return s.body.Close() }

// Comments returns how many comment lines Next has read so far. It may be
// called while another goroutine waits in Next.
func (s *DeveloperStream) Comments() int { return int(s.comments.Load()) }

// Next returns the stream's next event.
func (s *DeveloperStream) Next() (DeveloperEvent, error) {
	buffer := make([]byte, 4096)
	for len(s.events) == 0 {
		n, err := s.body.Read(buffer)
		s.events = s.frames.feed(buffer[:n], nil)
		s.comments.Store(int64(s.frames.comments))
		if err != nil && len(s.events) == 0 {
			return DeveloperEvent{}, err
		}
	}
	event := s.events[0]
	s.events = s.events[1:]
	return event, nil
}

func (d *Developer) send(ctx context.Context, method, target string, header http.Header, body []byte) (*http.Response, error) {
	d.t.Helper()
	request, err := http.NewRequestWithContext(ctx, method, d.BaseURL+target, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for name, values := range header {
		request.Header[name] = values
	}
	if d.invalid {
		request = InvalidRequest(request)
	}
	if d.Authorize != nil {
		if err := d.Authorize(request); err != nil {
			return nil, err
		}
	}
	client := *d.HTTP
	if client.Transport == nil {
		client.Transport = http.DefaultTransport
	}
	client.Transport = d.Transport(client.Transport)
	return client.Do(request)
}

// Transport returns base with every request, answer and stream event held
// to the file.
func (d *Developer) Transport(base http.RoundTripper) http.RoundTripper {
	return roundTrip(func(request *http.Request) (*http.Response, error) {
		d.t.Helper()
		if !public(request) {
			return base.RoundTrip(request)
		}
		forward, streamed := request, request.Context().Value(streamedRequest{}) != nil
		var body []byte
		if !streamed {
			var err error
			if body, err = requestBody(request); err != nil {
				return nil, err
			}
			forward = request.Clone(request.Context())
			forward.Body = io.NopCloser(bytes.NewReader(body))
		}
		sent := d.sent(request, body, !streamed)
		response, err := base.RoundTrip(forward)
		if err != nil {
			return response, err
		}
		return response, d.received(sent, response)
	})
}

// Exchange holds to the file a request a test wrote itself as bytes on a
// socket, so that no HTTP client merges or reorders its header lines, and
// the answer the test read back, as Transport holds them. A nil answer, for
// a request whose answer the test never reads, holds the request alone.
func (d *Developer) Exchange(request *http.Request, response *http.Response) {
	d.t.Helper()
	body, err := requestBody(request)
	if err != nil {
		d.report("%s %s: the request's body: %v", request.Method, request.URL.RequestURI(), err)
	}
	request.Body = io.NopCloser(bytes.NewReader(body))
	sent := d.sent(request, body, true)
	switch {
	case !sent.public:
	case response == nil:
		if !sent.invalid && len(sent.outside) > 0 {
			d.report("%s: request outside the file: %s", sent.what, strings.Join(sent.outside, "; "))
		}
	default:
		if err := d.received(sent, response); err != nil {
			d.report("%s: the answer's body: %v", sent.what, err)
		}
	}
}

// received holds a public request's answer to the file. A stream's events
// are held as the caller reads them; any other body is read whole and put
// back for the caller.
func (d *Developer) received(sent developerRequest, response *http.Response) error {
	d.t.Helper()
	if !sent.public {
		return nil
	}
	sent.answered(response.StatusCode)
	if stream := d.stream(sent.what, sent.operation, response.StatusCode, response.Header); stream != nil {
		response.Body = &checkedBody{ReadCloser: response.Body, stream: stream}
		return nil
	}
	if isEventStream(response.Header) {
		d.report("%s: answer %d is an event stream the file does not declare", sent.what, response.StatusCode)
		return nil
	}
	data, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	d.answered(sent, response.StatusCode, response.Header, data)
	response.Body = io.NopCloser(bytes.NewReader(data))
	return err
}

// Handler returns handler, a router a test calls in process, with every
// request, answer and stream event held to the file.
func (d *Developer) Handler(handler http.Handler) http.Handler {
	return checkedHandler{d: d, handler: handler}
}

type checkedHandler struct {
	d       *Developer
	handler http.Handler
}

func (h checkedHandler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	d := h.d
	d.t.Helper()
	body, err := requestBody(request)
	if err != nil {
		d.report("%s %s: the request's body: %v", request.Method, request.URL.RequestURI(), err)
	}
	request.Body = io.NopCloser(bytes.NewReader(body))
	checked := &checkedWriter{ResponseWriter: w, d: d, sent: d.sent(request, body, true)}
	h.handler.ServeHTTP(checked, request)
	checked.WriteHeader(http.StatusOK)
	if checked.stream == nil && checked.sent.public {
		d.answered(checked.sent, checked.status, checked.header, checked.body.Bytes())
	}
}

func requestBody(request *http.Request) ([]byte, error) {
	if request.Body == nil {
		return nil, nil
	}
	defer func() { _ = request.Body.Close() }()
	return io.ReadAll(request.Body)
}

func isEventStream(header http.Header) bool {
	media, _, _ := mime.ParseMediaType(header.Get("Content-Type"))
	return media == "text/event-stream"
}

// checkedWriter holds what a handler writes to the file: a stream's events
// as they are written, any other answer once the handler returns.
type checkedWriter struct {
	http.ResponseWriter
	d      *Developer
	sent   developerRequest
	status int
	header http.Header
	body   bytes.Buffer
	stream *checkedStream
}

func (w *checkedWriter) WriteHeader(status int) {
	w.d.t.Helper()
	if w.status == 0 && status >= http.StatusOK {
		w.status, w.header = status, w.ResponseWriter.Header().Clone()
		if !w.sent.public {
			w.ResponseWriter.WriteHeader(status)
			return
		}
		w.sent.answered(status)
		if w.stream = w.d.stream(w.sent.what, w.sent.operation, status, w.header); w.stream == nil && isEventStream(w.header) {
			w.d.report("%s: answer %d is an event stream the file does not declare", w.sent.what, status)
		}
		w.ResponseWriter.WriteHeader(status)
	}
}

func (w *checkedWriter) Write(data []byte) (int, error) {
	w.d.t.Helper()
	w.WriteHeader(http.StatusOK)
	if w.stream != nil {
		w.stream.written(data)
	} else {
		w.body.Write(data)
	}
	return w.ResponseWriter.Write(data)
}

func (w *checkedWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// Unwrap lets http.ResponseController reach the writer's deadlines.
func (w *checkedWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

type checkedBody struct {
	io.ReadCloser
	stream *checkedStream
}

func (b *checkedBody) Read(buffer []byte) (int, error) {
	n, err := b.ReadCloser.Read(buffer)
	b.stream.written(buffer[:n])
	return n, err
}

// frames splits the bytes of an event stream into its events. A frame of
// comment lines alone carries no event.
type frames struct {
	line     []byte
	event    DeveloperEvent
	data     []string
	names    int
	open     bool
	comments int
}

// feed returns the events data completes; outside, when given, is told each
// line that is neither a comment nor one of the two fields the file gives an
// event, and each frame that is not one event line and one data line.
func (f *frames) feed(data []byte, outside func(problem string)) []DeveloperEvent {
	var events []DeveloperEvent
	for _, b := range data {
		if b != '\n' {
			f.line = append(f.line, b)
			continue
		}
		line := strings.TrimSuffix(string(f.line), "\r")
		f.line = f.line[:0]
		if line == "" {
			if f.open {
				if outside != nil && (f.names != 1 || len(f.data) != 1) {
					outside(fmt.Sprintf("a stream frame has %d event and %d data lines, not one of each", f.names, len(f.data)))
				}
				f.event.Data = []byte(strings.Join(f.data, "\n"))
				events = append(events, f.event)
			}
			f.event, f.data, f.names, f.open = DeveloperEvent{}, nil, 0, false
			continue
		}
		name, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch name {
		case "":
			f.comments++
		case "event":
			f.event.Name, f.names, f.open = value, f.names+1, true
		case "data":
			f.data, f.open = append(f.data, value), true
		default:
			if outside != nil {
				outside(fmt.Sprintf("a stream line %q is neither event, data nor a comment", abbreviate([]byte(line))))
			}
		}
	}
	return events
}

// checkedStream holds the events of one stream answer to its operation.
type checkedStream struct {
	d         *Developer
	what      string
	operation *apiOperation
	frames    frames
}

// stream returns the check of a 200 answer that is one of the operation's
// streams, after checking its headers; nil for any other answer.
func (d *Developer) stream(what string, operation *apiOperation, status int, header http.Header) *checkedStream {
	d.t.Helper()
	if operation == nil || status != http.StatusOK || operation.responses["200"].events == nil {
		return nil
	}
	d.headers(what, operation, status, header)
	if !isEventStream(header) {
		d.report("%s: a stream's answer is %q, not text/event-stream", what, header.Get("Content-Type"))
	}
	record(operation, "200", "", "")
	return &checkedStream{d: d, what: what, operation: operation}
}

func (s *checkedStream) written(data []byte) {
	s.d.t.Helper()
	events := s.frames.feed(data, func(problem string) {
		s.d.report("%s: %s", s.what, problem)
	})
	for _, event := range events {
		if !s.operation.responses["200"].events[event.Name] {
			s.d.report("%s: stream event %q is not one %s declares", s.what, event.Name, s.operation.id)
			continue
		}
		value, err := jsonschema.UnmarshalJSON(bytes.NewReader(event.Data))
		if err != nil {
			s.d.report("%s: stream event %s holds no JSON: %s", s.what, event.Name, abbreviate(event.Data))
			continue
		}
		pointer := s.operation.pointer + "/responses/200/content/text~1event-stream/x-events/" + escapePointer(event.Name)
		if err := loadedAPI().validate(pointer, value); err != nil {
			s.d.report("%s: stream event %s of %s is outside the file: %v: %s", s.what, event.Name, s.operation.id, err, abbreviate(event.Data))
		}
		record(s.operation, "", "", event.Name)
	}
}

func loadedAPI() *apiSpec {
	spec, err := loadAPI()
	if err != nil {
		panic(fmt.Sprintf("enginetest: Engine's OpenAPI file: %v", err))
	}
	return spec
}

// developerRequest is a request with what of it is outside the file.
type developerRequest struct {
	d         *Developer
	what      string
	operation *apiOperation
	invalid   bool
	outside   []string
	// undeclared is the status the file's rule gives the request when it
	// declares no operation for it.
	undeclared int
	// refused is true for a path the file's path handling refuses.
	refused bool
	// public is false for a path outside /v1, such as a health probe, and
	// for the machine's routes under /v1/client, whose contract is
	// protocol/: the file says nothing of them and nothing is checked.
	public bool
}

// public reports whether the file speaks of a request's path.
func public(request *http.Request) bool {
	return strings.HasPrefix(request.URL.Path, "/v1/") && !strings.HasPrefix(request.URL.Path, "/v1/client/")
}

// sent holds a request to the file: its name in reports, its operation
// (nil for a path the file does not declare) and what of it is outside,
// its body included when held.
func (d *Developer) sent(request *http.Request, body []byte, held bool) developerRequest {
	sent := developerRequest{d: d, what: request.Method + " " + request.URL.RequestURI() + sentAt(), invalid: request.Context().Value(invalidRequest{}) != nil}
	if sent.public = public(request); !sent.public {
		return sent
	}
	spec := loadedAPI()
	var pathValues map[string]string
	target := request.URL.EscapedPath()
	if request.RequestURI != "" {
		// A raw exchange records the target it sent as it was written,
		// which a malformed path's URL cannot hold.
		target, _, _ = strings.Cut(request.RequestURI, "?")
	}
	path, refused := normalizedPath(target)
	if refused {
		sent.refused, sent.outside = true, []string{"the file's path handling refuses the path " + target}
		return sent
	}
	if sent.operation, pathValues = spec.operation(request.Method, path); sent.operation == nil {
		sent.outside = []string{"the file declares no " + request.Method + " " + request.URL.Path}
		sent.undeclared = spec.undeclaredStatus(request.Method, path)
	} else {
		sent.outside = requestOutside(spec, sent.operation, pathValues, request, body, held)
	}
	return sent
}

// normalizedPath is escaped, a request's path, as the file's x-path-handling
// states the edge routes it: a percent-encoded unreserved character decoded,
// dot segments removed (RFC 3986, section 6.2.2) and repeated slashes merged.
// refused is true for a path it refuses: one with an invalid percent-encoding
// or an escaped slash or backslash.
func normalizedPath(escaped string) (path string, refused bool) {
	var decoded strings.Builder
	for i := 0; i < len(escaped); i++ {
		if escaped[i] != '%' {
			decoded.WriteByte(escaped[i])
			continue
		}
		if i+2 >= len(escaped) {
			return "", true
		}
		value, err := strconv.ParseUint(escaped[i+1:i+3], 16, 8)
		switch {
		case err != nil || value == '/' || value == '\\':
			return "", true
		case unreserved(byte(value)):
			decoded.WriteByte(byte(value))
		default:
			decoded.WriteString(escaped[i : i+3])
		}
		i += 2
	}
	in, out := strings.Split(decoded.String(), "/"), []string{""}
	for i, segment := range in[1:] {
		last := i == len(in)-2
		switch segment {
		case ".":
		case "..":
			if len(out) > 1 {
				out = out[:len(out)-1]
			}
		case "":
			if !last {
				continue
			}
		default:
			out = append(out, segment)
			continue
		}
		if last {
			out = append(out, "")
		}
	}
	return strings.Join(out, "/"), false
}

func unreserved(c byte) bool {
	return 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' || strings.IndexByte("-._~", c) >= 0
}

// sentAt names the line of the test a request was sent from, which a
// report's own position does not when a helper or the test's middleware
// stands between.
func sentAt() string {
	callers := make([]uintptr, 64)
	frames := runtime.CallersFrames(callers[:runtime.Callers(2, callers)])
	at := ""
	for {
		frame, more := frames.Next()
		if strings.HasSuffix(frame.File, "_test.go") {
			at = fmt.Sprintf(" (%s:%d)", filepath.Base(frame.File), frame.Line)
		}
		if !more {
			return at
		}
	}
}

// answered reports the request once its answer's status is known: one
// outside the file and not marked, or marked and not refused.
func (s developerRequest) answered(status int) {
	s.d.t.Helper()
	switch {
	case !s.invalid && len(s.outside) > 0:
		s.d.report("%s: request outside the file, answered %d: %s", s.what, status, strings.Join(s.outside, "; "))
	case s.invalid && status < http.StatusBadRequest:
		s.d.report("%s: request is marked invalid and was answered %d", s.what, status)
	}
}

// requestOutside returns what of a request is outside its operation, its
// body included when held.
func requestOutside(spec *apiSpec, operation *apiOperation, pathValues map[string]string, request *http.Request, body []byte, held bool) []string {
	var outside []string
	query := request.URL.Query()
	declared := map[string]bool{}
	for i, parameter := range operation.parameters {
		var value string
		var present bool
		switch parameter.in {
		case "path":
			value, present = pathValues[parameter.name]
		case "query":
			declared[parameter.name] = true
			value, present = query.Get(parameter.name), query.Has(parameter.name)
		case "header":
			value, present = request.Header.Get(parameter.name), request.Header.Get(parameter.name) != ""
		}
		switch {
		case !present && parameter.required:
			outside = append(outside, fmt.Sprintf("%s parameter %s is missing", parameter.in, parameter.name))
		case present && parameter.array:
			values := make([]any, len(query[parameter.name]))
			for i, text := range query[parameter.name] {
				values[i] = text
			}
			if err := spec.validate(fmt.Sprintf("%s/parameters/%d/schema", operation.pointer, i), values); err != nil {
				outside = append(outside, fmt.Sprintf("%s parameter %s: %v", parameter.in, parameter.name, err))
			}
		case present && parameter.schema:
			// A parameter is text; a schema of a number or a boolean
			// reads its text, any other schema the text itself.
			pointer := fmt.Sprintf("%s/parameters/%d/schema", operation.pointer, i)
			if err := spec.validate(pointer, parameterValue(value)); err != nil && spec.validate(pointer, value) != nil {
				outside = append(outside, fmt.Sprintf("%s parameter %s: %v", parameter.in, parameter.name, err))
			}
		}
	}
	if operation.closed {
		for name := range query {
			if !declared[name] {
				outside = append(outside, "query parameter "+name+" is not declared")
			}
		}
	}
	if !held {
		return outside
	}
	media, parameters, _ := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if media == "" && operation.body["application/json"] && operation.id != tokenExchange {
		// Engine reads a JSON body whatever its Content-Type, but for Auth's
		// token exchange, which requires application/json.
		media = "application/json"
	}
	switch {
	case len(body) == 0 && operation.required:
		outside = append(outside, "the body is missing")
	case len(body) == 0:
	case len(operation.body) == 0:
		outside = append(outside, "the operation takes no body")
	case !operation.body[media]:
		outside = append(outside, fmt.Sprintf("the body's type %q is not one of %v", media, slices.Sorted(maps.Keys(operation.body))))
	case media == "application/json":
		value, err := jsonschema.UnmarshalJSON(bytes.NewReader(body))
		if err != nil {
			return append(outside, "the body is no JSON")
		}
		if err := spec.validate(operation.pointer+"/requestBody/content/application~1json/schema", value); err != nil {
			outside = append(outside, "body: "+err.Error())
		}
	case media == "multipart/form-data":
		fields, err := formFields(body, parameters["boundary"])
		if err != nil {
			return append(outside, "the body is no form: "+err.Error())
		}
		if err := spec.validate(operation.pointer+"/requestBody/content/multipart~1form-data/schema", fields); err != nil {
			outside = append(outside, "form: "+err.Error())
		}
	}
	return outside
}

// parameterValue reads a parameter's text as the JSON value it spells, the
// text itself when it spells none.
func parameterValue(text string) any {
	value, err := jsonschema.UnmarshalJSON(strings.NewReader(text))
	if err != nil {
		return text
	}
	switch value.(type) {
	case json.Number, bool:
		return value
	}
	return text
}

// formFields is a form's fields as an object: a file field is its bytes'
// text; a field sent twice, or named with a trailing [], is an array.
func formFields(body []byte, boundary string) (map[string]any, error) {
	reader := multipart.NewReader(bytes.NewReader(body), boundary)
	fields := map[string]any{}
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			return fields, nil
		}
		if err != nil {
			return nil, err
		}
		data, err := io.ReadAll(part)
		if err != nil {
			return nil, err
		}
		name := part.FormName()
		switch held := fields[name].(type) {
		case nil:
			if strings.HasSuffix(name, "[]") {
				fields[name] = []any{string(data)}
			} else {
				fields[name] = string(data)
			}
		case []any:
			fields[name] = append(held, string(data))
		default:
			fields[name] = []any{held, string(data)}
		}
	}
}

// tokenExchange is the operation ID of Auth's token exchange.
const tokenExchange = "oauth.token" //nolint:gosec // G101: an operation ID, not credential material.

// headerValues holds the values the file's descriptions of its response
// headers give; request-id has none, any ID is Engine's.
var headerValues = map[string]*regexp.Regexp{
	"Cache-Control":  regexp.MustCompile(`^no-store$`),
	"Retry-After":    regexp.MustCompile(`^[0-9]+$`),
	"x-should-retry": regexp.MustCompile(`^false$`),
}

// headers reports each header the answer's status declares and the answer
// lacks or gives another value than the file's.
func (d *Developer) headers(what string, operation *apiOperation, status int, header http.Header) {
	d.t.Helper()
	for _, name := range operation.responses[fmt.Sprint(status)].headers {
		value := header.Get(name)
		switch {
		case value == "":
			d.report("%s: answer %d of %s lacks the header %s", what, status, operation.id, name)
		case headerValues[name] != nil && !headerValues[name].MatchString(value):
			d.report("%s: answer %d of %s has %s %q, not %s", what, status, operation.id, name, value, headerValues[name])
		}
	}
}

// declares returns the operation's answer of status, and whether it declares
// one; a nil operation declares none.
func (o *apiOperation) declares(status string) (apiResponse, bool) {
	if o == nil {
		return apiResponse{}, false
	}
	answer, declared := o.responses[status]
	return answer, declared
}

// carried reports whether header holds every header the answer declares.
func (a apiResponse) carried(header http.Header) bool {
	for _, name := range a.headers {
		if header.Get(name) == "" {
			return false
		}
	}
	return true
}

// edgeAnswer holds an answer to the edge's own answer of status the file
// states: an ErrorResponse of the status's error type whose request_id is
// its request-id header.
func (s *apiSpec) edgeAnswer(status, media string, header http.Header, body []byte) error {
	if media != "application/json" {
		return fmt.Errorf("the edge's answer is %q, not application/json: %s", media, abbreviate(body))
	}
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("the edge's answer holds no JSON: %s", abbreviate(body))
	}
	if err := s.validate("/components/schemas/ErrorResponse", value); err != nil {
		return fmt.Errorf("the edge's answer is no ErrorResponse: %v: %s", err, abbreviate(body))
	}
	errorType, requestID := errorAnswer(body)
	if want := s.edgeTypes[status]; errorType != want {
		return fmt.Errorf("the edge's answer is %s, not %s", errorType, want)
	}
	if requestID == "" || header.Get("request-id") != requestID {
		return fmt.Errorf("the edge's answer repeats request %q under request-id %q", requestID, header.Get("request-id"))
	}
	return nil
}

// errorAnswer is an error answer's type and request ID.
func errorAnswer(body []byte) (errorType, requestID string) {
	var failure struct {
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
		RequestID string `json:"request_id"`
	}
	_ = json.Unmarshal(body, &failure)
	return failure.Error.Type, failure.RequestID
}

// sameRequestID reports an error answer whose request_id is not its
// request-id header.
func (d *Developer) sameRequestID(what string, code int, header http.Header, requestID string) {
	d.t.Helper()
	if id := header.Get("request-id"); id != "" && requestID != id {
		d.report("%s: error answer %d repeats request %q, not its request-id %q", what, code, requestID, id)
	}
}

// answered holds an answer that is no stream to its operation, or to the
// file's rule for a path it does not declare.
func (d *Developer) answered(sent developerRequest, code int, header http.Header, body []byte) {
	d.t.Helper()
	what, operation := sent.what, sent.operation
	status, spec := fmt.Sprint(code), loadedAPI()
	media, _, _ := mime.ParseMediaType(header.Get("Content-Type"))
	edge := slices.Contains(spec.edge, status)
	if sent.refused {
		if code != http.StatusBadRequest || !edge {
			d.report("%s: answer %d to a path the file's path handling refuses; the edge answers it 400", what, code)
		} else if err := spec.edgeAnswer(status, media, header, body); err != nil {
			d.report("%s: answer %d to a path the file's path handling refuses: %v", what, code, err)
		}
		return
	}
	answer, declared := operation.declares(status)
	if edge && code != http.StatusBadRequest && (!declared || !answer.carried(header)) {
		// The edge's answer while a service behind it cannot be reached,
		// whatever the operation. An answer with every header the operation
		// declares for the status is the operation's own.
		err := spec.edgeAnswer(status, media, header, body)
		if err == nil {
			return
		}
		if !declared {
			d.report("%s: answer %d is no operation's and not the edge's: %v", what, code, err)
			return
		}
	}
	if operation == nil {
		// 401 answers a request without valid credentials, whatever its path.
		errorTypes := map[int]string{http.StatusUnauthorized: "authentication_error", http.StatusBadRequest: "invalid_request_error"}
		if code != http.StatusUnauthorized && code != sent.undeclared {
			d.report("%s: answer %d to a path the file does not declare; its rule gives %d", what, code, sent.undeclared)
			return
		}
		if want := errorTypes[code]; want != "" {
			errorType, requestID := errorAnswer(body)
			if errorType != want {
				d.report("%s: answer %d to a path the file does not declare is no %s: %s", what, code, want, abbreviate(body))
			}
			d.sameRequestID(what, code, header, requestID)
		}
		return
	}
	if !declared {
		d.report("%s: answer %d is not one %s declares: %s", what, code, operation.id, abbreviate(body))
		return
	}
	d.headers(what, operation, code, header)
	hasSchema, known := answer.content[media]
	if _, wildcard := answer.content["*/*"]; wildcard {
		known = true
	}
	errorType := ""
	switch {
	case len(answer.content) == 0:
		if len(body) > 0 {
			d.report("%s: answer %d of %s has a body where the file has none: %s", what, code, operation.id, abbreviate(body))
		}
	case !known:
		d.report("%s: answer %d of %s is %q, not one of the file's types", what, code, operation.id, media)
	case media == "application/json" && hasSchema:
		value, err := jsonschema.UnmarshalJSON(bytes.NewReader(body))
		if err != nil {
			d.report("%s: answer %d of %s holds no JSON: %s", what, code, operation.id, abbreviate(body))
			return
		}
		pointer := operation.pointer + "/responses/" + status + "/content/application~1json/schema"
		if err := spec.validate(pointer, value); err != nil {
			d.report("%s: answer %d of %s is outside the file: %v: %s", what, code, operation.id, err, abbreviate(body))
		}
		if answer.errorTypes != nil {
			var requestID string
			if errorType, requestID = errorAnswer(body); !slices.Contains(answer.errorTypes, errorType) {
				d.report("%s: error %s under %d is not one %s declares (%v)", what, errorType, code, operation.id, answer.errorTypes)
			}
			d.sameRequestID(what, code, header, requestID)
		}
	}
	record(operation, status, errorType, "")
}

var coverage struct {
	sync.Mutex
	file *os.File
	seen map[string]bool
}

// record appends what was checked to the coverage file, each line once per
// process.
func record(operation *apiOperation, status, errorType, event string) {
	path := os.Getenv(engineapi.CoverageFileEnv)
	if path == "" || operation == nil {
		return
	}
	line, _ := json.Marshal(engineapi.Checked{Operation: operation.id, Status: status, Error: errorType, Event: event})
	coverage.Lock()
	defer coverage.Unlock()
	if coverage.seen[string(line)] {
		return
	}
	if coverage.file == nil {
		// The test runner names the record file.
		//nolint:gosec
		file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return
		}
		coverage.file, coverage.seen = file, map[string]bool{}
	}
	coverage.seen[string(line)] = true
	_, _ = coverage.file.Write(append(line, '\n'))
}

// Target is path with query, when it has any.
func Target(path string, query url.Values) string {
	if len(query) == 0 {
		return path
	}
	return path + "?" + query.Encode()
}
